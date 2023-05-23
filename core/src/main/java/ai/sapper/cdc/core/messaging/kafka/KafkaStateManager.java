package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.core.state.OffsetStateManager;
import ai.sapper.cdc.core.state.OffsetStateManagerSettings;
import ai.sapper.cdc.core.state.StateManagerError;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class KafkaStateManager extends OffsetStateManager<KafkaOffset> {

    @Override
    public OffsetStateManager<KafkaOffset> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            super.init(xmlConfig, env, KafkaConsumerOffsetSettings.class);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Throwable ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new StateManagerError(ex);
        }
    }

    @Override
    public OffsetStateManager<KafkaOffset> init(@NonNull OffsetStateManagerSettings settings,
                                                @NonNull BaseEnv<?> env) throws StateManagerError {
        try {
            setup(settings, env);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Throwable ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new StateManagerError(ex);
        }
    }

    public KafkaConsumerState get(@NonNull String topic, int partition) throws StateManagerError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        Preconditions.checkArgument(partition >= 0);
        String name = String.format("%s/%d", topic, partition);
        return get(KafkaConsumerState.OFFSET_TYPE, name, KafkaConsumerState.class);
    }

    public KafkaConsumerState create(@NonNull String topic, int partition) throws StateManagerError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        Preconditions.checkArgument(partition >= 0);
        String name = String.format("%s/%d", topic, partition);
        KafkaConsumerState state = create(KafkaConsumerState.OFFSET_TYPE, name, KafkaConsumerState.class);
        state.setTopic(topic);
        state.setPartition(partition);
        KafkaOffset offset = new KafkaOffset();
        offset.setTopic(topic);
        offset.setPartition(partition);
        state.setOffset(offset);

        return update(state);
    }

    public KafkaConsumerState update(@NonNull KafkaConsumerState offset) throws StateManagerError {
        return super.update(offset);
    }
}
