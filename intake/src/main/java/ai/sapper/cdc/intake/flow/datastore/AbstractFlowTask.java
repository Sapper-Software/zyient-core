package ai.sapper.cdc.intake.flow.datastore;

import ai.sapper.cdc.common.config.ConfigPath;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.intake.flow.ETaskResponse;
import ai.sapper.cdc.intake.flow.FlowTaskException;
import ai.sapper.cdc.intake.flow.TaskContext;
import ai.sapper.cdc.intake.flow.TaskResponse;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.annotation.Nonnull;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class AbstractFlowTask<K, T> {
    private FlowTaskSettings settings;
    private TaskGroup<K, T, ?> group;
    @Getter(AccessLevel.NONE)
    private List<AbstractFlowTask<K, T>> tasks;

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    public AbstractFlowTask<K, T> withSettings(@Nonnull FlowTaskSettings settings) {
        this.settings = settings;
        return this;
    }

    public AbstractFlowTask<K, T> withTaskGroup(@Nonnull TaskGroup<K, T, ?> group) {
        this.group = group;

        return this;
    }

    public TaskResponse run(T data, @Nonnull TaskContext context, @Nonnull Principal user) throws FlowTaskException {
        Preconditions.checkNotNull(settings);
        long startt = System.currentTimeMillis();
        DefaultLogger.info(String.format("Running Task Step [%s]: " +
                "[id=%s][start time=%d]", settings.getName(), context.getTaskId(), startt));
        DefaultLogger.trace(data);
        TaskResponse response = null;
        try {
            response = process(data, context, user);
            if (response == null) {
                throw new FlowTaskException("Process returned null response.");
            }
            if (tasks != null) {
                for (AbstractFlowTask<K, T> task : tasks) {
                    if (task.doProcess(context, data)) {
                        response = group.runTask(task, data, context, user);
                        if (response.getState() == ETaskResponse.StopWithError
                                || response.getState() == ETaskResponse.Stop) {
                            break;
                        } else if (response.getState() == ETaskResponse.Error) {
                            throw new FlowTaskException(response.getError());
                        }
                    }
                }
            }
            DefaultLogger.info(String.format("Finished Task Step [%s]: " +
                            "[id=%s][response=%s]", name, context.getTaskId(),
                    response.getState().name()));
        } catch (Throwable e) {
            DefaultLogger.error(getClass().getCanonicalName(), e);
            if (response == null) response = new TaskResponse();
            response.error(e);
        } finally {
            DefaultLogger.info(String.format("Finished Task Step [%s]: " +
                            "[id=%s][time taken=%d]", settings.getName(), context.getTaskId(),
                    (System.currentTimeMillis() - startt)));
        }
        return response;
    }

    public void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                          @Nonnull BaseEnv<?> env) throws ConfigurationException {
        if (node instanceof ConfigPathNode) {
            setup(node);
            AbstractConfigNode cnode = ConfigUtils.getPathNode(getClass(), (ConfigPathNode) node);
            if (cnode != null && group != null) {
                AbstractConfigNode tsnode = node.find(TaskGroup.CONFIG_NODE_TASKS);
                if (tsnode instanceof ConfigPathNode) {
                    tasks = new ArrayList<>();
                    group.configTasks((ConfigPathNode) tsnode, tasks);
                }
            }
        }
    }

    public boolean doProcess(@Nonnull TaskContext context, @Nonnull T data) {
        return true;
    }

    public abstract void setup(@Nonnull AbstractConfigNode node) throws ConfigurationException;

    public abstract TaskResponse process(T data, TaskContext context, @Nonnull Principal user) throws FlowTaskException;
}
