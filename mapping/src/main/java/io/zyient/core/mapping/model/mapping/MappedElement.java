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

package io.zyient.core.mapping.model.mapping;

import io.zyient.base.common.config.Config;
import io.zyient.base.common.model.entity.EDataTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class MappedElement extends Mapped {
    @Config(name = "[@source]")
    private String sourcePath;
    @Config(name = "target")
    private String targetPath;
    @Config(name = "nullable", required = false, type = Boolean.class)
    private boolean nullable = true;
    private Class<?> type;
    @Config(name = "type", required = false, type = MappingType.class)
    private MappingType mappingType = MappingType.Field;
    @Config(name = "dataType", required = false, type = EDataTypes.class)
    private EDataTypes dataType = null;
    @Config(name = "targetType", required = false, type = Class.class)
    private Class<?> targetType = null;

    public MappedElement() {

    }

    public MappedElement(@NonNull String sourcePath,
                         @NonNull String targetPath,
                         boolean nullable,
                         @NonNull Class<?> type,
                         MappingType mappingType) {
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.nullable = nullable;
        this.type = type;
        if (mappingType != null)
            this.mappingType = mappingType;
    }

    public MappedElement(@NonNull MappedElement source) {
        sourcePath = source.sourcePath;
        targetPath = source.targetPath;
        nullable = source.nullable;
        type = source.type;
        mappingType = source.mappingType;
        dataType = source.dataType;
        targetType = source.targetType;
    }
}
