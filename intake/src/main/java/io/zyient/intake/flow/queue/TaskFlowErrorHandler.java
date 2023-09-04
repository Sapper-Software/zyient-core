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

package io.zyient.intake.flow.queue;

import com.codekutter.zconfig.common.IConfigurable;
import com.codekutter.zconfig.common.model.annotations.ConfigPath;
import com.ingestion.common.flow.FlowTaskException;
import com.ingestion.common.flow.TaskContext;
import com.ingestion.common.flow.TaskResponse;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.security.Principal;

@Getter
@Setter
@ConfigPath(path = "error-handler")
public abstract class TaskFlowErrorHandler<T> implements IConfigurable {
    private TaskQueue<?, T, ?, ?> parent = null;

    public TaskFlowErrorHandler<T> withTaskGroup(@Nonnull TaskQueue<?, T, ?, ?> parent) {
        this.parent = parent;

        return this;
    }

    public abstract void handleError(@Nonnull TaskContext context,
                                     @Nonnull TaskResponse response,
                                     @Nonnull T data,
                                     @Nonnull Principal user) throws FlowTaskException;
}
