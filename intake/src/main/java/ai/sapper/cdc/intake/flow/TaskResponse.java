package ai.sapper.cdc.intake.flow;

import com.codekutter.common.AbstractState;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TaskResponse extends AbstractState<ETaskResponse> {
    private Throwable nonFatalError;

    public TaskResponse() {
        setState(ETaskResponse.Unknown);
    }
}
