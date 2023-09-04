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

import io.zyient.base.common.model.entity.IKey;
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
