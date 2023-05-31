package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.MessagingProcessorSettings;
import ai.sapper.cdc.core.processing.MessageProcessor;
import ai.sapper.cdc.core.processing.ProcessingState;
import ai.sapper.cdc.core.state.Offset;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public abstract class KafkaMessageProcessor<E extends Enum<?>, O extends Offset, M> extends MessageProcessor<String, M, E, O, KafkaOffset> {

    protected KafkaMessageProcessor(@NonNull BaseEnv<?> env,
                                    @NonNull Class<? extends ProcessingState<E, O>> stateType,
                                    @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(env, stateType, settingsType);
    }

    protected KafkaMessageProcessor(@NonNull BaseEnv<?> env,
                                    @NonNull Class<? extends ProcessingState<E, O>> stateType) {
        super(env, stateType);
    }
}
