package ai.sapper.cdc.intake.flow;

import com.codekutter.common.model.IKey;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Embeddable;

@Getter
@Setter
@Embeddable
public class TaskAuditParamId implements IKey {
    @Column(name = "task_id")
    private String taskId;
    @Column(name = "param")
    private String param;

    public TaskAuditParamId() {

    }

    public TaskAuditParamId(@Nonnull String taskId, @Nonnull String param) {
        this.taskId = taskId;
        this.param = param;
    }

    @Override
    public String stringKey() {
        return String.format("%s::%s", taskId, param);
    }

    @Override
    public int compareTo(IKey iKey) {
        int ret = -1;
        if (iKey instanceof TaskAuditParamId) {
            ret = taskId.compareTo(((TaskAuditParamId) iKey).taskId);
            if (ret == 0) {
                ret = param.compareTo(((TaskAuditParamId) iKey).param);
            }
        }
        return ret;
    }
}
