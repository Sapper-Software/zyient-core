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

package io.zyient.base.core.mapping.transformers;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.DataException;
import lombok.NonNull;

public class FloatTransformer extends NumericTransformer<Float> {

    public FloatTransformer() {
        super(Float.class);
    }

    @Override
    public Float transform(@NonNull Object source) throws DataException {
        if (ReflectionUtils.isNumericType(source.getClass())) {
            return (float) source;
        } else if (source instanceof String value) {
            if (Strings.isNullOrEmpty(value)) {
                return null;
            }
            Number number = parse(value);
            if (number != null) {
                return number.floatValue();
            }
        }
        throw new DataException(String.format("Cannot transform to Float. [source=%s]", source.getClass()));
    }

    @Override
    public String write(@NonNull Float source) throws DataException {
        return format.format(source);
    }
}
