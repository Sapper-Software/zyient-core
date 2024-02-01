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

package io.zyient.core.mapping.mapper.db;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EDataTypes;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.StringKey;
import io.zyient.core.mapping.decisions.ConditionType;
import io.zyient.core.mapping.model.mapping.MappingType;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@MappedSuperclass
public abstract class DBConditionDef implements IEntity<StringKey> {
    @EmbeddedId
    private StringKey id;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "key", column = @Column(name = "parent_id"))
    })
    private StringKey parentId;
    @Column(name = "source_field")
    private String field;
    @Column(name = "condition_string")
    private String condition;
    @Enumerated(EnumType.STRING)
    @Column(name = "data_type")
    private EDataTypes dataType;
    @Enumerated(EnumType.STRING)
    @Column(name = "condition_type")
    private ConditionType type = ConditionType.Simple;
    @Transient
    private List<DBMapping> mappings;

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(StringKey key) {
        return id.compareTo(key);
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public StringKey entityKey() {
        return id;
    }

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     * <p>
     * All properties other than the Key will be copied.
     * Copy Type:
     * Primitive - Copy
     * String - Copy
     * Enum - Copy
     * Nested Entity - Copy Recursive
     * Other Objects - Copy Reference.
     *
     * @param source  - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @throws CopyException
     */
    @Override
    public IEntity<StringKey> copyChanges(IEntity<StringKey> source, Context context) throws CopyException {
        throw new CopyException("Method not supported...");
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<StringKey> clone(Context context) throws CopyException {
        throw new CopyException("Method not supported...");
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        errors = ValidationExceptions.checkValue(id.getKey(),
                "Condition ID is null/empty", errors);
        errors = ValidationExceptions.checkCondition(type != null,
                "Condition type not set.", errors);
        if (type == ConditionType.Simple)
            errors = ValidationExceptions.checkValue(field,
                    "Missing required field: [field]", errors);
        errors = ValidationExceptions.checkValue(condition,
                "Missing required field: [condition]", errors);
        errors = ValidationExceptions.checkCondition(dataType != null,
                "Missing required field: [type]", errors);
        if (errors != null)
            throw errors;
    }

    public DBMappingDef add(@NonNull Class<? extends DBMappingDef> type,
                            String sourcePath,
                            @NonNull String targetPath,
                            @NonNull MappingType mappingType,
                            boolean nullable) throws Exception {
        if (mappings == null) {
            mappings = new ArrayList<>();
        }
        DBMappingId id = new DBMappingId();
        id.setConditionId(this.id.getKey());
        id.setSequence(mappings.size());
        DBMappingDef def = type.getDeclaredConstructor()
                        .newInstance();
        def.setId(id);
        def.setSourcePath(sourcePath);
        def.setTargetPath(targetPath);
        def.setMappingType(mappingType);
        def.setNullable(nullable);
        mappings.add(def);

        return def;
    }
}
