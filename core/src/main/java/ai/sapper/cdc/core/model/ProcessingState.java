package ai.sapper.cdc.core.model;

import ai.sapper.cdc.core.state.OffsetState;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public abstract class ProcessingState<O extends OffsetState<?, ?>> {
    private ModuleInstance instance;
    private String namespace;
    private O processedOffset;
    private long updatedTime;

    public ProcessingState() {
    }

    public ProcessingState(@NonNull ProcessingState<O> state) {
        this.instance = state.instance;
        this.namespace = state.namespace;
        this.processedOffset = state.processedOffset;
        this.updatedTime = state.updatedTime;
    }

    public int compareTx(@NonNull O target) {
        if (processedOffset != null) {
            return processedOffset.getOffset().compareTo(target.getOffset());
        }
        return -1;
    }
}
