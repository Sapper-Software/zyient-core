package ai.sapper.cdc.intake.flow.queue;

import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.ingestion.common.flow.FlowTaskException;
import com.ingestion.common.flow.TaskContext;
import com.ingestion.common.flow.TaskResponse;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.security.Principal;

@Getter
@Setter
@ConfigPath(path = "error-handler")
public abstract class TaskFlowErrorHandler<T> implements IConfigurable {
    private TaskQueue<?, T, ?, ?> parent = null;

    public TaskFlowErrorHandler<T> withTaskGroup(@Nonnull TaskQueue<?, T, ?, ?> parent) {
        this.parent = parent;

        return this;
    }

    public abstract void handleError(@Nonnull TaskContext context,
                                     @Nonnull TaskResponse response,
                                     @Nonnull T data,
                                     @Nonnull Principal user) throws FlowTaskException;
}
