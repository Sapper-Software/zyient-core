package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverBuilder;
import ai.sapper.cdc.core.processing.ProcessStateManager;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.core.state.Offset;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageProcessor<K, M, E extends Enum<?>, O extends Offset> implements Runnable {
    protected final Logger LOG;
    protected final ProcessorState state = new ProcessorState();

    private final ProcessStateManager<E, O> stateManager;
    private MessageReceiver<K, M> receiver;
    private BaseEnv<?> env;
    private MessagingConfig receiverConfig;
    private MessageProcessorState<E, O> processorState;

    protected MessageProcessor(@NonNull ProcessStateManager<E, O> stateManager) {
        this.stateManager = stateManager;
        this.LOG = LoggerFactory.getLogger(getClass());
    }

    @SuppressWarnings("unchecked")
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
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
            MessageReceiverBuilder<K, M> builder = (MessageReceiverBuilder<K, M>) settings.getBuilderType()
                    .getDeclaredConstructor(BaseEnv.class)
                    .newInstance(env);
            receiver = builder.build(config);
            processorState = (MessageProcessorState<E, O>) stateManager.processingState();
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

            state.setState(ProcessorState.EProcessorState.Stopped);
        } catch (Throwable t) {
            state.error(t);
            DefaultLogger.error(LOG, "Message Processor terminated with error", t);
            DefaultLogger.stacktrace(t);
            BaseEnv.remove(env.name());
        }
    }

    private void doRun() throws Exception {

    }
}
