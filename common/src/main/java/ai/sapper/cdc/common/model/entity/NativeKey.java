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

package ai.sapper.cdc.common.model.entity;

import ai.sapper.cdc.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public abstract class NativeKey<T> implements IKey {
    private final Class<? extends T> type;

    public NativeKey(@NonNull Class<? extends T> type) {
        Preconditions.checkArgument(ReflectionUtils.isPrimitiveTypeOrClass(type));
        this.type = type;
    }
}
