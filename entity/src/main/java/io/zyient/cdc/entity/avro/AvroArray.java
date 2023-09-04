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

package io.zyient.cdc.entity.avro;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.cdc.entity.types.DataType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;

import java.sql.Types;
import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AvroArray<I> extends DataType<List> {
    private Class<? extends I> inner;

    public AvroArray() {
        super("array", List.class, Types.ARRAY);
    }

    @SuppressWarnings("unchecked")
    public AvroArray(@NonNull Schema.Field field) {
        super("array", List.class, Types.ARRAY);
        Preconditions.checkArgument(field.schema().getType() == Schema.Type.ARRAY);
        Schema in = field.schema().getElementType();
        DataType<?> dt = AvroEntitySchema.getNativeDatatype(in);
        if (dt == null) {
            dt = AvroEntitySchema.RECORD;
        }
        this.inner = (Class<? extends I>) dt.getJavaType();
    }
}
