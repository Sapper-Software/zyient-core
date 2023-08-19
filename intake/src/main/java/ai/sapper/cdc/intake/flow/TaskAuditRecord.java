package ai.sapper.cdc.intake.flow;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Entity
@Table(name = "audit_task_records")
public class TaskAuditRecord implements IEntity<TaskAuditId> {
    @EmbeddedId
    private TaskAuditId taskId;
    @Column(name = "task_group")
    private String taskGroup;
    @Column(name = "task_name")
    private String taskName;
    @Column(name = "source")
    private String source;
    @Column(name = "correlation_id")
    private String correlationId;
    @Column(name = "start_time")
    private long startTime;
    @Column(name = "end_time")
    private long endTime;
    @Column(name = "task_step")
    private String step;
    @Column(name = "step_update_timestamp")
    private long stepUpdateTimestamp;
    @Column(name = "task_state")
    @Enumerated(EnumType.STRING)
    private ETaskResponse taskState;
    @Column(name = "error")
    private String error;
    @OneToMany(
            cascade = CascadeType.ALL,
            orphanRemoval = true
    )
    @JoinColumn(name = "task_id")
    @MapKey(name = "id.param")
    private Map<String, TaskAuditParam> params;

    @Override
    public int compare(TaskAuditId taskId) {
        return this.taskId.compareTo(taskId);
    }

    @Override
    public IEntity<TaskAuditId> copyChanges(IEntity<TaskAuditId> iEntity, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<TaskAuditId> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public TaskAuditId getKey() {
        return taskId;
    }

    public TaskAuditParam addParam(@Nonnull String name, @Nonnull String value) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

        TaskAuditParam param = null;
        if (params == null || !params.containsKey(name)) {
            TaskAuditParamId pid = new TaskAuditParamId(taskId.getTaskId(), name);
            param = new TaskAuditParam();
            param.setId(pid);
            if (params == null) params = new HashMap<>();
            params.put(name, param);
        } else {
            param = params.get(name);
        }
        param.setValue(value);

        return param;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
