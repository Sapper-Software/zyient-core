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

package ai.sapper.cdc.entity.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Getter
@Setter
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class DataType<T> {
    private String name;
    private Class<? extends T> javaType;
    private int jdbcType;

    public DataType() {
    }

    public DataType(@NonNull String name,
                    @NonNull Class<? extends T> javaType,
                    int jdbcType) {
        this.name = name;
        this.javaType = javaType;
        this.jdbcType = jdbcType;
    }

    public DataType(@NonNull DataType<T> source) {
        this.name = source.name;
        this.javaType = source.javaType;
        this.jdbcType = source.jdbcType;
    }

    public boolean compare(@NonNull String name) {
        return this.name.compareToIgnoreCase(name) == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataType<?> dataType = (DataType<?>) o;
        return name.equals(dataType.name) && javaType.equals(dataType.javaType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, javaType);
    }

    public boolean isType(@NonNull DataType<?> type) {
        return (name.equals(type.name) && javaType.equals(type.javaType));
    }
}
