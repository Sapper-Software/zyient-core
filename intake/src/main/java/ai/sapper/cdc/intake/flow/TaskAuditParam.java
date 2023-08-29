package ai.sapper.cdc.intake.flow;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.common.model.CopyException;
import ai.sapper.cdc.common.model.ValidationExceptions;
import ai.sapper.cdc.common.model.entity.IEntity;
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
        try {
            if (iEntity instanceof TaskAuditParam source) {
                id = source.id;
                value = source.value;
                return this;
            } else {
                throw new CopyException(
                        String.format("Invalid entity type: [type=%s]", iEntity.getClass().getCanonicalName()));
            }
        } catch (Exception ex) {
            throw new CopyException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public IEntity<TaskAuditParamId> clone(Context context) throws CopyException {
       try {
           return (IEntity<TaskAuditParamId>) clone();
       } catch (Exception ex) {
           throw new CopyException(ex);
       }
    }

    @Override
    public TaskAuditParamId getKey() {
        return id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
