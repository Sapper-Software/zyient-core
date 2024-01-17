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

package io.zyient.core.mapping.mapper.db;

import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EDataTypes;
import io.zyient.core.mapping.model.mapping.MappedElement;
import io.zyient.core.mapping.model.mapping.MappingType;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "m_mapping_defs")
public class DBMappingDef extends DBMapping {
    @EmbeddedId
    private DBMappingId id;
    @Column(name = "source_path")
    private String sourcePath;
    @Column(name = "target_path")
    private String targetPath;
    @Enumerated(EnumType.STRING)
    @Column(name = "mapping_type")
    private MappingType mappingType;
    @Column(name = "nullable")
    private boolean nullable = true;
    @Enumerated(EnumType.STRING)
    @Column(name = "data_type")
    private EDataTypes dataType = EDataTypes.String;

    public DBMappingDef() {
    }

    public DBMappingDef(@NonNull DBMappingDef source) {
        id = source.id;
        sourcePath = source.sourcePath;
        targetPath = source.targetPath;
        mappingType = source.mappingType;
        nullable = source.nullable;
        dataType = source.dataType;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        errors = ValidationExceptions.checkValue(id.getConditionId(),
                "Condition ID is null/empty", errors);
        errors = ValidationExceptions.chack(id.getSequence() >= 0,
                "Invalid ID sequence...", errors);
        errors = ValidationExceptions.checkValue(sourcePath,
                "Source Path is null/empty...", errors);
        errors = ValidationExceptions.checkValue(targetPath,
                "Target Path is null/empty...", errors);
        errors = ValidationExceptions.chack(mappingType != null,
                "Mapping Type not set...", errors);
        errors = ValidationExceptions.chack(dataType != null,
                "Data Type not set...", errors);
        if (errors != null) {
            throw errors;
        }
    }

    public MappedElement as() throws Exception {
        return new MappedElement(sourcePath,
                targetPath,
                nullable,
                EDataTypes.asJavaType(dataType),
                mappingType);
    }
}
