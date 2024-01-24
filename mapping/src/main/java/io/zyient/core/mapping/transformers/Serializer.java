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

package io.zyient.core.mapping.transformers;

import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.zyient.core.mapping.mapper.MappingSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

@Getter
@Accessors(fluent = true)
public abstract class Serializer<T> extends StdSerializer<T> {
    protected String name;

    protected Serializer(@NonNull Class<T> t) {
        super(t);
    }

    public abstract Serializer<T> configure(@NonNull MappingSettings settings) throws ConfigurationException;

}
