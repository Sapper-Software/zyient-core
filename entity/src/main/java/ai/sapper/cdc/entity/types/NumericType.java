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

import ai.sapper.cdc.common.utils.ReflectionUtils;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.math.BigDecimal;
import java.math.BigInteger;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class NumericType<T> extends DataType<T> {

    public NumericType() {
    }

    public NumericType(@NonNull String name,
                       @NonNull Class<? extends T> javaType,
                       int jdbcType) {
        super(name, javaType, jdbcType);
        Preconditions.checkArgument(ReflectionUtils.isNumericType(javaType)
                || javaType.equals(BigInteger.class)
                || javaType.equals(BigDecimal.class));
    }

    public NumericType(@NonNull DataType<T> source) {
        super(source);
        Preconditions.checkArgument(ReflectionUtils.isNumericType(getJavaType())
                || getJavaType().equals(BigInteger.class)
                || getJavaType().equals(BigDecimal.class));
    }
}
