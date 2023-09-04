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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.cdc.entity.types.DataType;
import io.zyient.cdc.entity.types.DataTypeUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Getter
@Setter
@ToString
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class SchemaField implements Comparable<SchemaField> {
    private String name;
    private DataType<?> dataType;
    private Object defaultVal;
    private int position;
    private boolean nullable = true;
    private boolean deleted = false;
    private String charset;
    private int jdbcType;

    public SchemaField() {
    }

    public SchemaField(@NonNull SchemaField field) {
        this.name = field.name;
        this.dataType = field.dataType;
        this.defaultVal = field.defaultVal;
        this.position = field.position;
        this.nullable = field.nullable;
    }

    @Override
    public boolean equals(Object o) {
        Preconditions.checkState(!Strings.isNullOrEmpty(name));
        Preconditions.checkNotNull(dataType);

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaField that = (SchemaField) o;
        return name.equals(that.name) && dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType);
    }

    /**
     * @param field
     * @return
     */
    @Override
    public int compareTo(@NonNull SchemaField field) {
        if (position >= 0) {
            return position - field.position;
        }
        return name.compareTo(field.getName());
    }

    public boolean isCompatible(@NonNull SchemaField target) {
        if (DataTypeUtils.isNullType(target)) return true;
        return DataTypeUtils.isCompatible(target.getDataType(), getDataType());
    }
}
