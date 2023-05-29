package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.*;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.state.Offset;
import ai.sapper.cdc.core.state.OffsetState;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;

public abstract class MessageProcessor<K, M, E extends Enum<?>, O extends Offset> extends Processor<E, O> {
    private MessageReceiver<K, M> receiver;
    private MessageSender<K, M> errorLogger;
    private MessagingConfig receiverConfig;
    private MessageProcessorState<E, O> processorState;

    protected MessageProcessor(@NonNull ProcessStateManager<E, O> stateManager) {
        super(stateManager);
    }

    @SuppressWarnings("unchecked")
    public Processor<E, O> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                String path,
                                @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
        HierarchicalConfiguration<ImmutableNode> config = xmlConfig;
        if (!Strings.isNullOrEmpty(path)) {
            config = xmlConfig.configurationAt(path);
        }
        receiverConfig = new MessagingConfig(config);
        receiverConfig.read();
        try {
            MessagingConfigSettings settings = (MessagingConfigSettings) receiverConfig.settings();
            super.init(settings);
            __lock().lock();
            try {
                HierarchicalConfiguration<ImmutableNode> qConfig = receiverConfig
                        .config()
                        .configurationAt(MessagingConfigSettings.Constants.__CONFIG_PATH_RECEIVER);
                if (qConfig == null) {
                    throw new ConfigurationException(
                            String.format("Receiver queue configuration not found. [path=%s]",
                                    MessagingConfigSettings.Constants.__CONFIG_PATH_RECEIVER));
                }
                MessageReceiverBuilder<K, M> builder = (MessageReceiverBuilder<K, M>) settings.getBuilderType()
                        .getDeclaredConstructor(BaseEnv.class, Class.class)
                        .newInstance(env, settings.getBuilderSettingsType());
                receiver = builder.build(qConfig);
                processorState = (MessageProcessorState<E, O>) stateManager.processingState();
                if (settings.getErrorsBuilderType() != null) {
                    HierarchicalConfiguration<ImmutableNode> eConfig = receiverConfig
                            .config()
                            .configurationAt(MessagingConfigSettings.Constants.__CONFIG_PATH_ERRORS);
                    if (eConfig == null) {
                        throw new ConfigurationException(
                                String.format("Errors queue configuration not found. [path=%s]",
                                        MessagingConfigSettings.Constants.__CONFIG_PATH_ERRORS));
                    }
                    MessageSenderBuilder<K, M> errorBuilder = (MessageSenderBuilder<K, M>) settings.getErrorsBuilderType()
                            .getDeclaredConstructor(BaseEnv.class, Class.class)
                            .newInstance(env, settings.getErrorsBuilderSettingsType());
                    errorLogger = errorBuilder.build(eConfig);
                }

                return this;
            } finally {
                __lock().unlock();
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void run() {
        Preconditions.checkState(state.isInitialized());
        Preconditions.checkNotNull(env);
        try {
            state.setState(ProcessorState.EProcessorState.Running);
            doRun();
            state.setState(ProcessorState.EProcessorState.Stopped);
        } catch (Throwable t) {
            state.error(t);
            DefaultLogger.error(LOG, "Message Processor terminated with error", t);
            DefaultLogger.stacktrace(t);
            try {
                BaseEnv.remove(env.name());
            } catch (Exception ex) {
                DefaultLogger.error(LOG, "Message Processor terminated with error", t);
                DefaultLogger.stacktrace(t);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void doRun() throws Throwable {
        MessagingConfigSettings settings = (MessagingConfigSettings) receiverConfig.settings();
        while (state.isAvailable()) {
            boolean sleep = false;
            __lock().lock();
            try {
                List<MessageObject<K, M>> batch = receiver.nextBatch(settings.getReceiveBatchTimeout());
                if (batch != null && !batch.isEmpty()) {
                    LOG.debug(String.format("Received messages. [count=%d]", batch.size()));
                    OffsetState<E, O> offsetState = (OffsetState<E, O>) receiver.currentOffset(null);
                    processorState.setMessageOffset(offsetState.getOffset());
                    stateManager.update(processorState);
                    for (MessageObject<K, M> message : batch) {
                        try {
                            process(message);
                        } catch (InvalidMessageError me) {
                            DefaultLogger.stacktrace(me);
                            DefaultLogger.warn(LOG, me.getLocalizedMessage());
                            if (errorLogger != null) {
                                errorLogger.send(message);
                            }
                        }
                    }
                } else {
                    sleep = true;
                }
            } finally {
                __lock().unlock();
            }
            if (sleep) {
                try {
                    Thread.sleep(settings.getReceiveBatchTimeout());
                } catch (InterruptedException e) {
                    LOG.info(String.format("[%s] Thread interrupted. [%s]",
                            settings.getName(), e.getLocalizedMessage()));
                }
            }
        }
    }

    protected abstract void process(@NonNull MessageObject<K, M> message) throws Exception;

    protected abstract void batchStart() throws Exception;

    protected abstract void batchEnd() throws Exception;
}
