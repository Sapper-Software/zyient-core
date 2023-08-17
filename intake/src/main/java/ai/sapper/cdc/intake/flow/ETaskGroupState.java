package ai.sapper.cdc.intake.flow;

import com.codekutter.common.IState;

public enum ETaskGroupState implements IState<ETaskGroupState> {
    Unknown, Initialized, Running, Stopped, Error;

    @Override
    public ETaskGroupState getErrorState() {
        return Error;
    }
}
