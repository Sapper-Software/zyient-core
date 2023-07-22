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

package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.core.executor.BaseTask;
import ai.sapper.cdc.core.state.BaseStateManager;
import ai.sapper.cdc.entity.manager.SchemaManager;
import ai.sapper.cdc.entity.model.EntityReadState;
import ai.sapper.cdc.entity.model.TransactionId;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class EntityTask<T extends TransactionId> extends BaseTask<T> {
    private final SchemaEntity entity;
    private EntityReadState<T> entityState;
    private final SchemaManager schemaManager;

    public EntityTask(@NonNull BaseStateManager stateManager,
                      @NonNull SchemaManager schemaManager,
                      @NonNull String type,
                      @NonNull SchemaEntity entity) {
        super(stateManager, type, entity.toString());
        this.schemaManager = schemaManager;
        this.entity = entity;
    }

    public EntityTask(@NonNull BaseStateManager stateManager,
                      @NonNull SchemaManager schemaManager,
                      @NonNull SchemaEntity entity) {
        super(stateManager, entity.toString());
        this.schemaManager = schemaManager;
        this.entity = entity;
    }
}
