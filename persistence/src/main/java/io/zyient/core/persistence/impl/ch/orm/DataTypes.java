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

package io.zyient.core.persistence.impl.ch.orm;

import lombok.Getter;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Getter
public enum DataTypes {
    UInt8(Short.class, false),
    UInt16(Short.class, false),
    UInt32(Integer.class, false),
    UInt64(Long.class, false),
    UInt128(BigInteger.class, false),
    UInt256(BigInteger.class, false),
    Int8(Short.class, false),
    Int16(Short.class, false),
    Int32(Integer.class, false),
    Int64(Long.class, false),
    Int128(BigInteger.class, false),
    Int256(BigInteger.class, false),
    Float32(Float.class, false),
    Float64(Double.class, false),
    Decimal(BigDecimal.class, true),
    Boolean(java.lang.Boolean.class, false),
    String(java.lang.String.class, false),
    FixedString(java.lang.String.class, true),
    Date(LocalDate.class, false),
    DateTime(LocalDateTime.class, false),
    JSON(Object.class, false);

    private final Class<?> javaType;
    private final boolean sized;

    DataTypes(@NonNull Class<?> javaType, boolean sized) {
        this.javaType = javaType;
        this.sized = sized;
    }

    public String type(Integer precision, Integer scale) throws Exception {
        if (sized && precision < 0) {
            throw new Exception(java.lang.String.format("Type requires size parameter. [type=%s]", name()));
        }
        if (sized) {
            if (precision < scale) {
                throw new Exception(java.lang.String.format("Precision cannot be less than scale. [precision=%d][scale=%s]",
                        precision, scale));
            }
            if (scale < 0) scale = 0;
            return java.lang.String.format("%s(%d, %d)", name(), precision, scale);
        } else {
            return name();
        }
    }
}
