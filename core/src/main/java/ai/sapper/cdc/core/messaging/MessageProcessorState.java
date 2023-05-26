package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.processing.ProcessingState;
import ai.sapper.cdc.core.state.Offset;
import ai.sapper.cdc.core.state.OffsetState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MessageProcessorState<E extends Enum<?>, O extends Offset> extends ProcessingState<E, O> {
    private Offset messageOffset;

    public MessageProcessorState(@NonNull E errorState) {
        super(errorState);
    }

    public MessageProcessorState(@NonNull MessageProcessorState<E, O> state) {
        super(state);
        messageOffset = state.getMessageOffset();
    }
}
