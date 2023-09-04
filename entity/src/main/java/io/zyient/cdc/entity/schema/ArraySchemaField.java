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
import java.util.List;
import java.util.Objects;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ArraySchemaField extends SchemaField {
    public static final DataType<List> ARRAY = new DataType<>("ARRAY", List.class, Types.ARRAY);

    private SchemaField field;

    public ArraySchemaField() {
        setDataType(ARRAY);
        setJdbcType(Types.ARRAY);
    }

    public ArraySchemaField(@NonNull ArraySchemaField source) {
        super(source);
        field = source.field;
    }

    /**
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            if (o instanceof ArraySchemaField) {
                return field.equals(((ArraySchemaField) o).field);
            }
        }
        return false;
    }

    /**
     * @return
     */
    @Override
    public int hashCode() {
        return Objects.hash(getName(), getDataType(), field.getDataType());
    }

    /**
     * @param target
     * @return
     */
    @Override
    public boolean isCompatible(@NonNull SchemaField target) {
        if (DataTypeUtils.isNullType(target)) return true;
        if (target instanceof ArraySchemaField) {
            return field.isCompatible(((ArraySchemaField) target).field);
        }
        return false;
    }
}
