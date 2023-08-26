package ai.sapper.cdc.intake.flow;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.common.StateException;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class TaskGroupState extends AbstractState<ETaskGroupState> {
    public TaskGroupState() {
       super(ETaskGroupState.Error, ETaskGroupState.Unknown);
    }

    @JsonIgnore
    public boolean isRunning() {
        return (getState() == ETaskGroupState.Running);
    }

    public void checkState(Class<?> caller, ETaskGroupState expected) throws StateException {
        if (getState() != expected) {
            throw new StateException(String.format("State Error : [expected=%s][current=%s]",
                    expected.name(), getState().name()));
        }
    }
}
