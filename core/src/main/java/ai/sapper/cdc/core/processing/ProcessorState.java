package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.AbstractEnvState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ProcessorState extends AbstractEnvState<ProcessorState.EProcessorState> {
    public ProcessorState() {
        super(EProcessorState.Error, EProcessorState.Initialized);
        setState(EProcessorState.Unknown);
    }

    @JsonIgnore
    public boolean isInitialized() {
        return (getState() == EProcessorState.Initialized
                || getState() == EProcessorState.Running);
    }

    @JsonIgnore
    @Override
    public boolean isAvailable() {
        return (getState() == EProcessorState.Running);
    }

    @JsonIgnore
    @Override
    public boolean isTerminated() {
        return (getState() == EProcessorState.Stopped || hasError());
    }

    public enum EProcessorState {
        Unknown, Initialized, Running, Stopped, Error, Paused;
    }
}
