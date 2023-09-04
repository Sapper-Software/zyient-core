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

package io.zyient.intake.flow.datastore;

import io.zyient.intake.flow.FlowTaskException;
import io.zyient.intake.flow.TaskContext;
import io.zyient.intake.flow.TaskResponse;
import io.zyient.base.core.BaseEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.annotation.Nonnull;
import java.security.Principal;

@Getter
@Setter
public abstract class TaskFlowErrorHandler<T> {
    private TaskGroup<?, T, ?> parent = null;
    private BaseEnv<?> env;

    public TaskFlowErrorHandler<T> withTaskGroup(@Nonnull TaskGroup<?, T, ?> parent) {
        this.parent = parent;
        return this;
    }

    public abstract void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                   @Nonnull BaseEnv<?> env) throws ConfigurationException;

    public abstract void handleError(@Nonnull TaskContext context,
                                     @Nonnull TaskResponse response,
                                     @Nonnull T data,
                                     @Nonnull Principal user) throws FlowTaskException;
}
