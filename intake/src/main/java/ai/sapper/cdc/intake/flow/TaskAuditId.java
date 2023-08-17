package ai.sapper.cdc.intake.flow;

import com.codekutter.common.model.IKey;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Getter
@Setter
@Embeddable
public class TaskAuditId implements IKey {
    @Column(name = "task_id")
    private String taskId;

    @Override
    public String stringKey() {
        return taskId;
    }

    @Override
    public int compareTo(IKey iKey) {
        if (iKey instanceof TaskAuditId) {
            return taskId.compareTo(((TaskAuditId) iKey).taskId);
        }
        return -1;
    }
}
