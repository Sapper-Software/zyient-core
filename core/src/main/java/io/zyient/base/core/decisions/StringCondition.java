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

import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.NonNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class StringCondition<T> extends BasicCondition<T> {
    private Pattern pattern = null;

    protected StringCondition() {
        super(String.class);
    }

    @Override
    public boolean evaluate(@NonNull T data) throws Exception {
        Object value = getValue(data);
        if (value != null) {
            if (!(value instanceof String dv)) {
                throw new Exception(String.format("Invalid value returned. [type=%s]",
                        value.getClass().getCanonicalName()));
            }
            String cv = (String) value();
            switch (op()) {
                case NotEq -> {
                    return dv.compareTo(cv) != 0;
                }
                case Lt -> {
                    return dv.compareTo(cv) < 0;
                }
                case GtEq -> {
                    return dv.compareTo(cv) >= 0;
                }
                case Eq -> {
                    return dv.compareTo(cv) == 0;
                }
                case Gt -> {
                    return dv.compareTo(cv) > 0;
                }
                case LtEq -> {
                    return dv.compareTo(cv) <= 0;
                }
                case Like -> {
                    if (pattern == null) {
                        throw new Exception("Pattern not initialized...");
                    }
                    Matcher m = pattern.matcher(dv);
                    return m.matches();
                }
                case NotNull -> {
                    return true;
                }
            }
        } else if (op() == Op.IsNull) {
            return true;
        }
        return false;
    }

    @Override
    public void validate() throws ValidationException {
        super.validate();
        if (!(value() instanceof String str)) {
            throw new ValidationException(String.format("Invalid predicate value. [type=%s]",
                    value().getClass().getCanonicalName()));
        }
        if (op() == Op.Like) {
            pattern = Pattern.compile(str);
        }
    }

    protected Object tryString(@NonNull Object value) {
        if (!(value instanceof String)) {
            if (ReflectionHelper.isPrimitiveTypeOrString(value.getClass())) {
                value = value.toString();
            } else if (value.getClass().isEnum()) {
                Enum<?> e = (Enum<?>) value;
                value = e.name();
            }
        }
        return value;
    }
}
