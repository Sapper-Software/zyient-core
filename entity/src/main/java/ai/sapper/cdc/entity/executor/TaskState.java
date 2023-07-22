package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.common.AbstractState;

public class TaskState extends AbstractState<ETaskState> {
    public TaskState() {
        super(ETaskState.ERROR, ETaskState.UNKNOWN);
        setState(ETaskState.UNKNOWN);
    }

    public boolean isRunning() {
        return getState() == ETaskState.RUNNING;
    }
}
