/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.NonNull;

public abstract class NumericCondition<T> extends BasicCondition<T> {
    protected NumericCondition(@NonNull Class<?> type) {
        super(type);
        Preconditions.checkArgument(ReflectionHelper.isNumericType(type));
    }

    @Override
    public boolean evaluate(@NonNull T data) throws Exception {
        Object value = getValue(data);
        if (value != null) {
            if (!ReflectionHelper.isNumericType(value.getClass())) {
                if (value instanceof String str) {
                    value = Double.parseDouble(str);
                } else
                    throw new Exception(String.format("Invalid value returned. [type=%s]",
                            value.getClass().getCanonicalName()));
            }
            double dv = (double) value;
            double cv = (double) value();
            switch (op()) {
                case Eq -> {
                    return dv == cv;
                }
                case Gt -> {
                    return dv > cv;
                }
                case Lt -> {
                    return dv < cv;
                }
                case GtEq -> {
                    return dv >= cv;
                }
                case LtEq -> {
                    return dv <= cv;
                }
                case NotEq -> {
                    return dv != cv;
                }
                case Like -> {
                    throw new Exception("Like not supported for numeric values...");
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
        if (!ReflectionHelper.isNumericType(value().getClass())) {
            throw new ValidationException(String.format("Invalid predicate value. [type=%s]",
                    value().getClass().getCanonicalName()));
        }
        if (op() == Op.Like) {
            throw new ValidationException("Like not supported for numeric values...");
        }
    }

    protected Object tryNumeric(@NonNull Object value) {
        if (value instanceof String str) {
            value = Double.parseDouble(str);
        }
        return value;
    }
}
