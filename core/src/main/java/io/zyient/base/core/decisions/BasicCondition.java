/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.decisions;

import com.google.common.base.Strings;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class BasicCondition<T> implements Condition<T> {
    private String property;
    private Op op;
    private final Class<?> type;
    private Object value;

    protected BasicCondition(@NonNull Class<?> type) {
        this.type = type;
    }

    @Override
    public void validate() throws ValidationException {
        if (Strings.isNullOrEmpty(property)) {
            throw new ValidationException("Property not specified...");
        }
        if (op == null) {
            throw new ValidationException("Operation not specified...");
        }
        if (!ReflectionHelper.isPrimitiveTypeOrString(type)) {
            if (!type.isEnum()) {
                throw new ValidationException(String.format("Type not supported. [type=%s]", type.getCanonicalName()));
            }
        }
        if (value == null) {
            throw new ValidationException("Value not specified...");
        }
    }

    protected abstract Object getValue(@NonNull T data) throws Exception;
}
