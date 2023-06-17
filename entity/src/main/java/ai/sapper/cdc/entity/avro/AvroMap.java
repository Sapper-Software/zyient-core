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

package ai.sapper.cdc.entity.avro;

import ai.sapper.cdc.entity.types.DataType;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;

import java.sql.Types;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AvroMap<V> extends DataType<Map> {
    private Class<? extends V> inner;

    public AvroMap() {
        super("map", Map.class, Types.STRUCT);
    }

    @SuppressWarnings("unchecked")
    public AvroMap(@NonNull Schema.Field field) {
        super("map", Map.class, Types.STRUCT);
        Preconditions.checkArgument(field.schema().getType() == Schema.Type.MAP);
        Schema in = field.schema().getValueType();
        DataType<?> dt = AvroEntitySchema.getNativeDatatype(in);
        if (dt == null) {
            dt = AvroEntitySchema.RECORD;
        }
        this.inner = (Class<? extends V>) dt.getJavaType();
    }
}
