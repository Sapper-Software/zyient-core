package ai.sapper.cdc.intake.flow;

import com.codekutter.common.IState;

public enum ETaskResponse implements IState<ETaskResponse> {
    Unknown, OK, Error, ContinueWithError, StopWithError, MoveToError, Stop, Running;


    /**
     * Get the state that represents an error state.
     *
     * @return - Error state.
     */
    @Override
    public ETaskResponse getErrorState() {
        return Error;
    }
}
