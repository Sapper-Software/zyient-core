package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.AbstractState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ProcessorState extends AbstractState<ProcessorState.EProcessorState> {
    public ProcessorState() {
        super(EProcessorState.Error);
        setState(EProcessorState.Unknown);
    }

    @JsonIgnore
    public boolean isInitialized() {
        return (getState() == EProcessorState.Initialized
                || getState() == EProcessorState.Running);
    }

    @JsonIgnore
    public boolean isRunning() {
        return (getState() == EProcessorState.Running);
    }

    public enum EProcessorState {
        Unknown, Initialized, Running, Stopped, Error, Paused;
    }
}
