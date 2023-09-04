/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.intake.flow;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.ReflectionUtils;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.*;
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

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     * <p>
     * All properties other than the Key will be copied.
     * Copy Type:
     * Primitive - Copy
     * String - Copy
     * Enum - Copy
     * Nested Entity - Copy Recursive
     * Other Objects - Copy Reference.
     *
     * @param source  - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @throws CopyException
     */
    @Override
    public IEntity<TaskAuditId> copyChanges(IEntity<TaskAuditId> source,
                                            Context context) throws CopyException {
        try {
            if (source instanceof TaskAuditRecord record) {
                params = new HashMap<>(record.params);
                taskId = record.taskId;
                ReflectionUtils.copyNatives(record, this);
                return this;
            } else {
                throw new Exception(
                        String.format("Invalid entity type: [type=%s]", source.getClass().getCanonicalName()));
            }
        } catch (Exception ex) {
            throw new CopyException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public IEntity<TaskAuditId> clone(Context context) throws CopyException {
        try {
            return (IEntity<TaskAuditId>) clone();
        } catch (Exception ex) {
            throw new CopyException(ex);
        }
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
