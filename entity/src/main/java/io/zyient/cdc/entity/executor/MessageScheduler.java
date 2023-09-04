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
import io.zyient.cdc.entity.model.TransactionId;
import io.zyient.base.core.executor.BaseScheduler;
import io.zyient.base.core.executor.BaseTask;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class MessageScheduler<T extends TransactionId> extends BaseScheduler<T> {
    @Override
    public void add(@NonNull BaseTask<T> task) {
        Preconditions.checkArgument(task instanceof MessageTask<?, ?, ?>);
        super.add(task);
    }
}
