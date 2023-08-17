package ai.sapper.cdc.intake.flow;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "audit_task_params")
public class TaskAuditParam implements IEntity<TaskAuditParamId> {
    @EmbeddedId
    private TaskAuditParamId id;
    @Column(name = "value")
    private String value;

    @Override
    public int compare(TaskAuditParamId taskAuditParamId) {
        return id.compareTo(taskAuditParamId);
    }

    @Override
    public IEntity<TaskAuditParamId> copyChanges(IEntity<TaskAuditParamId> iEntity, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<TaskAuditParamId> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public TaskAuditParamId getKey() {
        return id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
