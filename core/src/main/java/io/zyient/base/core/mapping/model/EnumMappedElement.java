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

package io.zyient.base.core.mapping.model;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@SuppressWarnings("rawtypes")
public class EnumMappedElement extends MappedElement {
    @Config(name = "enum", type = Class.class)
    private Class<? extends Enum> enumType;
    @Config(name = "mappings", required = false, type = Map.class)
    private Map<String, String> enumMappings;

    public String getName() {
        Preconditions.checkNotNull(enumType);
        return enumType.getSimpleName();
    }
}
