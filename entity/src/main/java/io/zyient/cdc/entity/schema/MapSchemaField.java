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

package io.zyient.cdc.entity.schema;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.cdc.entity.types.DataType;
import io.zyient.cdc.entity.types.DataTypeUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.sql.Types;
import java.util.Map;
import java.util.Objects;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MapSchemaField extends SchemaField {
    public static final DataType<Map> MAP = new DataType<>("MAP", Map.class, Types.STRUCT);

    public static final int JDBC_TYPE = Integer.MIN_VALUE + 1;

    private DataType<?> keyType;
    private SchemaField valueField;

    public MapSchemaField() {
        setDataType(MAP);
        setJdbcType(JDBC_TYPE);
    }

    public MapSchemaField(@NonNull MapSchemaField source) {
        super(source);
        keyType = source.keyType;
        valueField = source.valueField;
    }

    /**
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            if (o instanceof MapSchemaField) {
                return (keyType.equals(((MapSchemaField) o).keyType) &&
                        valueField.equals(((MapSchemaField) o).valueField));
            }
        }
        return false;
    }

    /**
     * @return
     */
    @Override
    public int hashCode() {
        return Objects.hash(getName(), getDataType(), keyType, valueField.getDataType());
    }

    /**
     * @param target
     * @return
     */
    @Override
    public boolean isCompatible(@NonNull SchemaField target) {
        if (DataTypeUtils.isNullType(target)) return true;
        if (target instanceof MapSchemaField) {
            if (DataTypeUtils.isCompatible(((MapSchemaField) target).keyType, keyType)) {
                return valueField.isCompatible(((MapSchemaField) target).valueField);
            }
        }
        return false;
    }
}
