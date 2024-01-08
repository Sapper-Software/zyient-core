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

package io.zyient.core.mapping.transformers;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.core.mapping.mapper.Mapping;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.net.URI;
import java.net.URL;
import java.util.Date;

@Getter
@Setter
@Accessors(fluent = true)
public class StringTransformer {
    private boolean useJson = true;
    private final Mapping<?> mapping;

    public StringTransformer(Mapping<?> mapping) {
        this.mapping = mapping;
    }

    public <T> String serialize(@NonNull T value) throws Exception {
        if (ReflectionHelper.isPrimitiveTypeOrString(value.getClass())) {
            return String.valueOf(value);
        } else if (value instanceof Date) {
            DateTransformer transformer = (DateTransformer) mapping.getDeSerializer(Date.class, null);
            Preconditions.checkNotNull(transformer);
            return transformer.serialize((Date) value);
        } else if (value.getClass().isEnum()) {
            return ((Enum<?>) value).name();
        } else if (useJson) {
            return JSONUtils.asString(value);
        } else if (value instanceof URI) {
            return ((URI) value).toString();
        } else if (value instanceof URL) {
            return ((URL) value).toString();
        } else {
            throw new Exception(String.format("Cannot map value to String. [type=%s]",
                    value.getClass().getCanonicalName()));
        }
    }

    public <T> T deserialize(@NonNull String value,
                             @NonNull Class<? extends T> type) throws Exception {
        if (!Strings.isNullOrEmpty(value)) {

        }
        return null;
    }
}
