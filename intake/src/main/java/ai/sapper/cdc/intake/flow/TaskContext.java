package ai.sapper.cdc.intake.flow;

import com.codekutter.common.Context;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

public class TaskContext extends Context {
    public static final String PARAM_TASK_ID = "task.group.id";
    public static final String PARAM_RUN_ID = "task.run.id";
    public static final String PARAM_TASK_START_TIME = "task.start.time";
    public static final String PARAM_TASK_END_TIME = "task.end.time";
    public static final String PARAM_CORRELACTION_ID = "task.correlation.id";

    public TaskContext() {

    }

    public TaskContext(@Nonnull Context source) {
        super(source);
    }

    public String getRunId() { return getStringParam(PARAM_RUN_ID);}

    public void setRunId(@Nonnull String runId) {
        setParam(PARAM_RUN_ID, runId);
    }

    public String getTaskId() {
        return getStringParam(PARAM_TASK_ID);
    }

    public void setTaskId(@Nonnull String taskId) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(taskId));
        setParam(PARAM_TASK_ID, taskId);
    }

    public long getTaskStartTime() {
        return getLongParam(PARAM_TASK_START_TIME);
    }

    public void setTaskStartTime(long starttime) {
        Preconditions.checkArgument(starttime > 0);
        setParam(PARAM_TASK_START_TIME, String.valueOf(starttime));
    }

    public long getTaskEndTime() {
        return getLongParam(PARAM_TASK_END_TIME);
    }

    public void setTaskEndTime(long endtime) {
        Preconditions.checkArgument(endtime > 0);
        setParam(PARAM_TASK_END_TIME, String.valueOf(endtime));
    }

    public String getCorrelationId() {
        return getStringParam(PARAM_CORRELACTION_ID);
    }

    public void setCorrelationId(@Nonnull String correlationId) {
        setParam(PARAM_CORRELACTION_ID, correlationId);
    }
}
