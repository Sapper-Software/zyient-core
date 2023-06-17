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

import java.util.Objects;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DecimalType<T> extends NumericType<T> {
    private int scale = 10;
    private int precision = 0;

    public DecimalType() {
    }

    public DecimalType(@NonNull String name,
                       @NonNull Class<? extends T> javaType,
                       int jdbcType) {
        super(name, javaType, jdbcType);
    }

    public DecimalType(@NonNull String name,
                       @NonNull Class<? extends T> javaType,
                       int jdbcType,
                       int scale,
                       int precision) {
        super(name, javaType, jdbcType);
        this.scale = scale;
        this.precision = precision;
    }

    public DecimalType(@NonNull DecimalType<T> source,
                       Integer scale,
                       Integer precision) {
        super(source);
        this.scale = Objects.requireNonNullElseGet(scale, () -> source.scale);
        this.precision = Objects.requireNonNullElseGet(precision, () -> source.precision);
    }

    /**
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (super.equals(o) && o instanceof DecimalType) {
            if (scale > 0 && ((DecimalType<?>) o).scale > 0) {
                if (scale != ((DecimalType<?>) o).scale) return false;
                if (precision > 0 && ((DecimalType<?>) o).precision > 0) {
                    return precision == ((DecimalType<?>) o).precision;
                }
            }
            return true;
        }
        return false;
    }
}
