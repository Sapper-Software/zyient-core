package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.core.messaging.ReceiverOffset;
import ai.sapper.cdc.core.state.Offset;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MessageProcessorState<E extends Enum<?>, O extends Offset, M extends ReceiverOffset> extends ProcessingState<E, O> {
    private M messageOffset;

    public MessageProcessorState(@NonNull E errorState,
                                 @NonNull E initState) {
        super(errorState, initState);
    }

    public MessageProcessorState(@NonNull MessageProcessorState<E, O, M> state) {
        super(state);
        messageOffset = state.getMessageOffset();
    }
}
