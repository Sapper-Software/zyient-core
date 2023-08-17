package ai.sapper.cdc.intake.flow.queue;

import com.codekutter.common.utils.LogUtils;
import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import com.google.common.base.Preconditions;
import com.ingestion.common.flow.FlowTaskException;
import com.ingestion.common.flow.TaskContext;
import com.ingestion.common.flow.TaskResponse;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.security.Principal;

@Getter
@Setter
@Accessors(fluent = true)
@ConfigPath(path = "task")
public abstract class AbstractFlowTask<K, T> implements IConfigurable {
    @ConfigValue(name = "name")
    private String name;
    @Setter(AccessLevel.NONE)
    private TaskQueue<K, T, ?, ?> group;

    public AbstractFlowTask<K, T> withTaskGroup(@Nonnull TaskQueue<K, T, ?, ?> group) {
        this.group = group;

        return this;
    }

    public TaskResponse run(T data, @Nonnull TaskContext context, @Nonnull Principal user) throws FlowTaskException {
        Preconditions.checkArgument(context != null);
        long startt = System.currentTimeMillis();
        LogUtils.info(getClass(), String.format("Running Task Step [%s]: " +
                "[id=%s][start time=%d]", name, context.getTaskId(), startt));
        LogUtils.debug(getClass(), data);
        try {
            TaskResponse response = process(data, context, user);
            if (response == null) {
                throw new FlowTaskException("Process returned null response.");
            }
            LogUtils.info(getClass(), String.format("Finished Task Step [%s]: " +
                            "[id=%s][response=%s]", name, context.getTaskId(),
                    response.getState().name()));
            return response;
        } catch (Exception e) {
            LogUtils.error(getClass(), e);
            throw new FlowTaskException(e);
        } finally {
            LogUtils.info(getClass(), String.format("Finished Task Step [%s]: " +
                            "[id=%s][time taken=%d]", name, context.getTaskId(),
                    (System.currentTimeMillis() - startt)));
        }
    }

    public abstract TaskResponse process(T data, TaskContext context, @Nonnull Principal user) throws FlowTaskException;
}
