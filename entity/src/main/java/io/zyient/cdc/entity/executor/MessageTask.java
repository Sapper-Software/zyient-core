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

package io.zyient.cdc.entity.executor;

import com.google.common.base.Preconditions;
import io.zyient.cdc.entity.manager.SchemaManager;
import io.zyient.cdc.entity.model.EntityReadState;
import io.zyient.cdc.entity.model.TransactionId;
import io.zyient.cdc.entity.schema.SchemaEntity;
import io.zyient.base.core.executor.BaseTask;
import io.zyient.base.core.executor.CompletionCallback;
import io.zyient.base.core.executor.TaskState;
import io.zyient.base.core.messaging.MessageReceiver;
import io.zyient.base.core.state.BaseStateManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class MessageTask<T extends TransactionId, I, M> extends EntityTask<T> {
    private final TaskState state = new TaskState();

    private final SchemaEntity entity;
    private EntityReadState<T> entityState;
    private final MessageReceiver<I, M> receiver;
    private final SchemaManager schemaManager;

    public MessageTask(@NonNull String type,
                       @NonNull SchemaEntity entity,
                       @NonNull MessageReceiver<I, M> receiver,
                       @NonNull BaseStateManager stateManager,
                       @NonNull SchemaManager schemaManager) {
        super(stateManager, schemaManager, type, entity);
        this.entity = entity;
        this.receiver = receiver;
        this.schemaManager = schemaManager;
    }

    public MessageTask(@NonNull SchemaEntity entity,
                       @NonNull MessageReceiver<I, M> receiver,
                       @NonNull BaseStateManager stateManager,
                       @NonNull SchemaManager schemaManager) {
        super(stateManager, schemaManager, entity);
        this.entity = entity;
        this.receiver = receiver;
        this.schemaManager = schemaManager;
    }

    @Override
    public BaseTask<T> withCallback(@NonNull CompletionCallback<T> callback) {
        Preconditions.checkArgument(callback instanceof MessageCompletionCallback<?>);
        return super.withCallback(callback);
    }
}
