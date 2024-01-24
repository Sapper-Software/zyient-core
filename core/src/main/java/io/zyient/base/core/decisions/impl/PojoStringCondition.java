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

package io.zyient.base.core.decisions.impl;

import io.zyient.base.common.utils.beans.BeanUtils;
import io.zyient.base.core.decisions.StringCondition;
import lombok.NonNull;

public class PojoStringCondition<T> extends StringCondition<T> {
    @Override
    protected Object getValue(@NonNull T data) throws Exception {
        Object value = BeanUtils.getValue(data, property());
        if (value != null) {
            return tryString(value);
        }
        return value;
    }
}
