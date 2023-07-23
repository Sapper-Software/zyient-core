/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.*;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.state.Offset;
import ai.sapper.cdc.core.state.OffsetState;
import ai.sapper.cdc.core.utils.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.List;

public abstract class MessageProcessor<K, M, E extends Enum<?>, O extends Offset, MO extends ReceiverOffset> extends Processor<E, O> {
    protected MessageReceiver<K, M> receiver;
    protected MessageSender<K, M> errorLogger;
    protected MessagingProcessorConfig receiverConfig;
    protected final Class<? extends MessagingProcessorSettings> settingsType;

    protected MessageProcessor(@NonNull BaseEnv<?> env,
                               @NonNull MessageProcessorMetrics metrics,
                               @NonNull Class<? extends ProcessingState<E, O>> stateType,
                               @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(env, metrics, stateType);
        this.settingsType = settingsType;
    }

    protected MessageProcessor(@NonNull BaseEnv<?> env,
                               @NonNull MessageProcessorMetrics metrics,
                               @NonNull Class<? extends ProcessingState<E, O>> stateType) {
        super(env, metrics, stateType);
        this.settingsType = null;
    }


    @Override
    @SuppressWarnings("unchecked")
    public Processor<E, O> init(@NonNull String name,
                                @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                String path) throws ConfigurationException {
        HierarchicalConfiguration<ImmutableNode> config = xmlConfig;
        if (!Strings.isNullOrEmpty(path)) {
            config = xmlConfig.configurationAt(path);
        }
        if (receiverConfig == null) {
            Preconditions.checkNotNull(settingsType);
            receiverConfig = new MessagingProcessorConfig(config, settingsType);
        }
        receiverConfig.read();
        try {
            MessagingProcessorSettings settings = (MessagingProcessorSettings) receiverConfig.settings();
            super.init(settings, name);
            __lock().lock();
            try {
                HierarchicalConfiguration<ImmutableNode> qConfig = receiverConfig
                        .config()
                        .configurationAt(MessagingProcessorSettings.Constants.__CONFIG_PATH_RECEIVER);
                if (qConfig == null) {
                    throw new ConfigurationException(
                            String.format("Receiver queue configuration not found. [path=%s]",
                                    MessagingProcessorSettings.Constants.__CONFIG_PATH_RECEIVER));
                }
                MessageReceiverBuilder<K, M> builder = (MessageReceiverBuilder<K, M>) settings.getBuilderType()
                        .getDeclaredConstructor(Class.class)
                        .newInstance(settings.getBuilderSettingsType());
                receiver = builder.withEnv(env).build(qConfig);
                if (settings.getErrorsBuilderType() != null) {
                    HierarchicalConfiguration<ImmutableNode> eConfig = receiverConfig
                            .config()
                            .configurationAt(MessagingProcessorSettings.Constants.__CONFIG_PATH_ERRORS);
                    if (eConfig == null) {
                        throw new ConfigurationException(
                                String.format("Errors queue configuration not found. [path=%s]",
                                        MessagingProcessorSettings.Constants.__CONFIG_PATH_ERRORS));
                    }
                    MessageSenderBuilder<K, M> errorBuilder = (MessageSenderBuilder<K, M>) settings.getErrorsBuilderType()
                            .getDeclaredConstructor(Class.class)
                            .newInstance(settings.getErrorsBuilderSettingsType());
                    errorLogger = errorBuilder.withEnv(env).build(eConfig);
                }

                postInit(settings);
                updateState();

                return this;
            } finally {
                __lock().unlock();
            }
        } catch (Exception ex) {
            try {
                updateError(ex);
            } catch (Throwable t) {
                DefaultLogger.stacktrace(t);
                DefaultLogger.error(LOG, "Failed to save state...", t);
            }
            throw new ConfigurationException(ex);
        }
    }


    @Override
    public void run() {
        Preconditions.checkState(state.isAvailable());
        Preconditions.checkNotNull(env);
        try {
            doRun(false);
        } catch (Throwable t) {
            state.error(t);
            DefaultLogger.error(LOG, "Message Processor terminated with error", t);
            DefaultLogger.stacktrace(t);
            try {
                updateError(t);
                BaseEnv.remove(env.name());
            } catch (Exception ex) {
                DefaultLogger.error(LOG, "Message Processor terminated with error", t);
                DefaultLogger.stacktrace(t);
            }
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    protected void doRun(boolean runOnce) throws Throwable {
        Preconditions.checkArgument(!runOnce);
        MessagingProcessorSettings settings = (MessagingProcessorSettings) receiverConfig.settings();
        while (state.isAvailable()) {
            boolean sleep = false;
            if (!state.isPaused()) {
                __lock().lock();
                try {
                    MessageProcessorState<E, O, MO> processorState = (MessageProcessorState<E, O, MO>) stateManager().processingState();
                    MO pOffset = processorState.getMessageOffset();
                    OffsetState<?, MO> offsetState = (OffsetState<?, MO>) receiver.currentOffset(null);
                    MO rOffset = offsetState.getOffset();
                    if (pOffset != null && pOffset.compareTo(rOffset) != 0) {
                        receiver.seek(pOffset, null);
                    }
                    List<MessageObject<K, M>> batch = receiver.nextBatch(settings.getReceiveBatchTimeout());
                    if (batch != null && !batch.isEmpty()) {
                        metrics.getCounter(MessageProcessorMetrics.METRIC_MESSAGES_READ).increment(batch.size());
                        try (Timer t = new Timer(metrics.getTimer(MessageProcessorMetrics.METRIC_BATCH_TIME))) {
                            LOG.debug(String.format("Received messages. [count=%d]", batch.size()));
                            offsetState = (OffsetState<?, MO>) receiver.currentOffset(null);
                            batchStart(processorState);
                            processorState = (MessageProcessorState<E, O, MO>) updateState();

                            handleBatch(batch, processorState);
                            processorState.setMessageOffset(offsetState.getOffset());

                            batchEnd(processorState);
                            updateState();
                        }
                    } else {
                        sleep = true;
                    }
                } finally {
                    __lock().unlock();
                }
            } else {
                sleep = true;
            }
            if (sleep) {
                try {
                    Thread.sleep(settings.getReceiveBatchTimeout());
                } catch (InterruptedException e) {
                    LOG.info(String.format("[%s] Thread interrupted. [%s]",
                            name(), e.getLocalizedMessage()));
                }
            }
        }
    }

    protected void handleBatch(@NonNull List<MessageObject<K, M>> batch,
                               @NonNull MessageProcessorState<E, O, MO> processorState) throws Exception {
        for (MessageObject<K, M> message : batch) {
            try (Timer t = new Timer(metrics.getTimer(MessageProcessorMetrics.METRIC_PROCESS_TIME))) {
                process(message, processorState);
                metrics.getCounter(MessageProcessorMetrics.METRIC_MESSAGES_PROCESSED).increment();
            } catch (InvalidMessageError me) {
                metrics.getCounter(MessageProcessorMetrics.METRIC_MESSAGES_ERROR).increment();
                DefaultLogger.stacktrace(me);
                DefaultLogger.warn(LOG, me.getLocalizedMessage());
                if (errorLogger != null) {
                    errorLogger.send(message);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (receiver != null) {
            receiver.close();
            receiver = null;
        }
        if (errorLogger != null) {
            errorLogger.close();
            errorLogger = null;
        }
    }

    protected abstract void process(@NonNull MessageObject<K, M> message,
                                    @NonNull MessageProcessorState<E, O, MO> processorState) throws Exception;

    protected abstract void postInit(@NonNull MessagingProcessorSettings settings) throws Exception;

    protected abstract void batchStart(@NonNull MessageProcessorState<E, O, MO> processorState) throws Exception;

    protected abstract void batchEnd(@NonNull MessageProcessorState<E, O, MO> processorState) throws Exception;
}
