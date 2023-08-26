package ai.sapper.cdc.intake.flow.datastore;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.intake.flow.FlowTaskException;
import ai.sapper.cdc.intake.flow.TaskContext;
import ai.sapper.cdc.intake.flow.TaskResponse;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.annotation.Nonnull;
import java.security.Principal;

@Getter
@Setter
public abstract class TaskFlowErrorHandler<T> {
    private TaskGroup<?, T, ?> parent = null;
    private BaseEnv<?> env;

    public TaskFlowErrorHandler<T> withTaskGroup(@Nonnull TaskGroup<?, T, ?> parent) {
        this.parent = parent;
        return this;
    }

    public abstract void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                   @Nonnull BaseEnv<?> env) throws ConfigurationException;

    public abstract void handleError(@Nonnull TaskContext context,
                                     @Nonnull TaskResponse response,
                                     @Nonnull T data,
                                     @Nonnull Principal user) throws FlowTaskException;
}
