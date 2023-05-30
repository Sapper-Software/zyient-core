package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.MessagingProcessorConfig;
import ai.sapper.cdc.core.messaging.kafka.BaseKafkaConsumer;
import ai.sapper.cdc.core.messaging.kafka.KafkaMessageProcessingState;
import ai.sapper.cdc.core.messaging.kafka.KafkaOffset;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public abstract class KafkaMessageProcessor<E extends Enum<?>, M> {
    private BaseKafkaConsumer<M> receiver;
    private KafkaMessageProcessingState<E> state;
    private BaseEnv<?> env;
    private MessagingProcessorConfig receiverConfig;
    private final ProcessStateManager<E, KafkaOffset> stateManager;

    protected KafkaMessageProcessor(@NonNull ProcessStateManager<E, KafkaOffset> stateManager) {
        this.stateManager = stateManager;
    }

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                     String path,
                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
    }
}
