/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.messaging.processing;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.processing.EventProcessorMetrics;
import io.zyient.base.core.processing.ProcessingState;
import io.zyient.base.core.processing.Processor;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.Offset;
import io.zyient.base.core.state.OffsetState;
import io.zyient.base.core.utils.Timer;
import io.zyient.core.messaging.*;
import io.zyient.core.messaging.builders.MessageReceiverBuilder;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public abstract class MessageProcessor<K, M, E extends Enum<?>, O extends Offset, MO extends ReceiverOffset<?>> extends Processor<E, O> {
    protected final Class<? extends MessagingProcessorSettings> settingsType;
    protected MessageReceiver<K, M> receiver;
    protected MessageSender<K, M> errorLogger;
    protected MessagingProcessorConfig receiverConfig;
    private boolean running = false;

    protected MessageProcessor(@NonNull Class<? extends ProcessingState<E, O>> stateType,
                               @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(stateType);
        this.settingsType = settingsType;
    }

    protected MessageProcessor(@NonNull Class<? extends ProcessingState<E, O>> stateType) {
        super(stateType);
        this.settingsType = null;
    }


    @Override
    @SuppressWarnings("unchecked")
    public Processor<E, O> init(@NonNull BaseEnv<?> env,
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
            super.init(settings, env);
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
                if (receiver.errors() != null) {
                    errorLogger = receiver.errors();
                }

                postInit(settings);
                updateState();
                env.withProcessor(this);

                EventProcessorMetrics metrics = new EventProcessorMetrics(getClass().getSimpleName(),
                        settings.getName(), receiver.connection().name(), env);
                return withMetrics(metrics);
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
            running = true;
            doRun(false);
        } catch (Throwable t) {
            state.error(t);
            DefaultLogger.error(LOG, "Message Processor terminated with error", t);
            DefaultLogger.stacktrace(t);
            try {
                updateError(t);
            } catch (Exception ex) {
                DefaultLogger.error(LOG, "Message Processor terminated with error", t);
                DefaultLogger.stacktrace(t);
            }
        } finally {
            running = false;
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
                    MessageProcessorState<E, O, MO> processorState
                            = (MessageProcessorState<E, O, MO>) stateManager().processingState(name());
                    MO pOffset = processorState.getMessageOffset();
                    OffsetState<?, MO> offsetState = (OffsetState<?, MO>) receiver.currentOffset(null);
                    MO rOffset = offsetState.getOffset();
                    if (pOffset != null && rOffset != null && pOffset.compareTo(rOffset) != 0) {
                        receiver.seek(pOffset, null);
                    }
                    List<MessageObject<K, M>> batch = receiver.nextBatch(settings.getReceiveBatchTimeout().normalized());
                    if (batch != null && !batch.isEmpty()) {
                        metrics.getCounter(EventProcessorMetrics.METRIC_EVENTS_READ).increment(batch.size());
                        try (Timer t = new Timer(metrics.getTimer(EventProcessorMetrics.METRIC_BATCH_TIME))) {
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
                    RunUtils.sleep(settings.getReceiveBatchTimeout().normalized());
                } catch (InterruptedException e) {
                    LOG.info(String.format("[%s] Thread interrupted. [%s]",
                            name(), e.getLocalizedMessage()));
                }
            }
        }
    }

    protected void handleBatch(@NonNull List<MessageObject<K, M>> batch,
                               @NonNull MessageProcessorState<E, O, MO> processorState) throws Exception {
        try {
            for (MessageObject<K, M> message : batch) {
                try (Timer t = new Timer(metrics.getTimer(EventProcessorMetrics.METRIC_EVENTS_TIME))) {
                    process(message, processorState);
                    metrics.getCounter(EventProcessorMetrics.METRIC_EVENTS_PROCESSED).increment();
                    receiver.ack(message.key(), false);
                } catch (InvalidMessageError | MessageProcessingError me) {
                    metrics.getCounter(EventProcessorMetrics.METRIC_EVENTS_ERROR).increment();
                    DefaultLogger.stacktrace(me);
                    DefaultLogger.warn(LOG, me.getLocalizedMessage());
                    receiver.ack(message.key(), false);
                    sendError(message);
                }
            }
        } finally {
            receiver.commit();
        }
    }

    public MessageObject<K, M> sendError(@NonNull MessageObject<K, M> message) throws Exception {
        if (errorLogger != null) {
            String id = message.id();
            message.id(UUID.randomUUID().toString());
            message.correlationId(id);
            message.mode(MessageObject.MessageMode.Error);
            errorLogger.send(message);
        }
        return message;
    }

    @Override
    public void close() throws IOException {
        try {
            if (running) {
                state.setState(ProcessorState.EProcessorState.Stopped);
                while (running) {
                    RunUtils.sleep(500);
                }
            }
            updateState();
            if (receiver != null) {
                receiver.close();
                receiver = null;
            }
            if (errorLogger != null) {
                errorLogger.close();
                errorLogger = null;
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(String.format("[%s] ERROR [%s]", settings().getName(), ex.getLocalizedMessage()));
            throw new IOException(ex);
        }
    }

    protected abstract void process(@NonNull MessageObject<K, M> message,
                                    @NonNull MessageProcessorState<E, O, MO> processorState) throws Exception;

    protected abstract void postInit(@NonNull MessagingProcessorSettings settings) throws Exception;

    protected abstract void batchStart(@NonNull MessageProcessorState<E, O, MO> processorState) throws Exception;

    protected abstract void batchEnd(@NonNull MessageProcessorState<E, O, MO> processorState) throws Exception;
}
