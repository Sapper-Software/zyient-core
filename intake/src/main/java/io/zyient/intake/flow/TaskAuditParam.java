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

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
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
