package ai.sapper.cdc.intake.flow;

import ai.sapper.cdc.common.AbstractState;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TaskResponse extends AbstractState<ETaskResponse> {
    private Throwable nonFatalError;

    public TaskResponse() {
        super(ETaskResponse.Error, ETaskResponse.Unknown);
    }
}
