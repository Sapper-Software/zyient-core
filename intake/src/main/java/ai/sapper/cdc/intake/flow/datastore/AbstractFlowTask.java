package ai.sapper.cdc.intake.flow.datastore;

import com.codekutter.common.utils.ConfigUtils;
import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.ConfigurationException;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.codekutter.zconfig.common.model.nodes.AbstractConfigNode;
import com.codekutter.zconfig.common.model.nodes.ConfigPathNode;
import com.google.common.base.Preconditions;
import com.ingestion.common.flow.ETaskResponse;
import com.ingestion.common.flow.FlowTaskException;
import com.ingestion.common.flow.TaskContext;
import com.ingestion.common.flow.TaskResponse;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "task")
public abstract class AbstractFlowTask<K, T> implements IConfigurable {
    @ConfigValue(name = "name")
    private String name;
    @Setter(AccessLevel.NONE)
    private TaskGroup<K, T, ?> group;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private List<AbstractFlowTask<K, T>> tasks;

    public AbstractFlowTask<K, T> withTaskGroup(@Nonnull TaskGroup<K, T, ?> group) {
        this.group = group;

        return this;
    }

    public TaskResponse run(T data, @Nonnull TaskContext context, @Nonnull Principal user) throws FlowTaskException {
        Preconditions.checkArgument(context != null);
        long startt = System.currentTimeMillis();
        LogUtils.info(getClass(), String.format("Running Task Step [%s]: " +
                "[id=%s][start time=%d]", name, context.getTaskId(), startt));
        LogUtils.debug(getClass(), data);
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
            LogUtils.info(getClass(), String.format("Finished Task Step [%s]: " +
                            "[id=%s][response=%s]", name, context.getTaskId(),
                    response.getState().name()));
        } catch (Throwable e) {
            LogUtils.error(getClass(), e);
            if (response == null) response = new TaskResponse();
            response.setError(e);
        } finally {
            LogUtils.info(getClass(), String.format("Finished Task Step [%s]: " +
                            "[id=%s][time taken=%d]", name, context.getTaskId(),
                    (System.currentTimeMillis() - startt)));
        }
        return response;
    }

    @Override
    public void configure(@Nonnull AbstractConfigNode node) throws ConfigurationException {
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
