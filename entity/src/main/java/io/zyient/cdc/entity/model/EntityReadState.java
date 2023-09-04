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

package io.zyient.cdc.entity.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.state.OffsetState;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class EntityReadState<T extends TransactionId> extends OffsetState<EEntityState, T> {
    public static final String OFFSET_TYPE = "entity/read";

    private String domain;
    private String entity;
    private T processedTxId;
    private String zkPath;
    private String queue;
    private String errorQueue;
    private long eventCount = 0;
    private long eventErrorCount = 0;
    private long editsEventCount = 0;

    public EntityReadState() {
        super(EEntityState.ERROR, EEntityState.UNKNOWN, OFFSET_TYPE);
    }

    public EntityReadState(@NonNull String type) {
        super(EEntityState.ERROR, EEntityState.UNKNOWN, type);
    }

    public boolean canProcess() {
        return (getState() == EEntityState.ACTIVE || getState() == EEntityState.SNAPSHOT);
    }
}
