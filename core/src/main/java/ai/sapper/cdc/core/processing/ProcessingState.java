package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.core.model.ModuleInstance;
import ai.sapper.cdc.core.state.Offset;
import ai.sapper.cdc.core.state.OffsetState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public abstract class ProcessingState<E extends Enum<?>, O extends Offset> extends OffsetState<E, O> {
    private ModuleInstance instance;
    private String namespace;

    public ProcessingState(@NonNull E errorState,
                           @NonNull E initState) {
        super(errorState, initState);
    }

    public ProcessingState(@NonNull ProcessingState<E, O> state) {
        super(state.getErrorState(), state.getInitState());
        this.instance = state.instance;
        this.namespace = state.namespace;
    }

    public int compareTx(@NonNull O target) {
        if (getOffset() != null) {
            return getOffset().compareTo(target);
        }
        return -1;
    }

    @Override
    public String toString() {
        return String.format("[MODULE=%s][namespace=%s]", instance, namespace);
    }
}
