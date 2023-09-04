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

package io.zyient.cdc.entity.types;

import io.zyient.cdc.entity.schema.SchemaField;
import io.zyient.base.common.utils.ReflectionUtils;
import lombok.NonNull;
import org.apache.commons.lang3.ObjectUtils;

import java.nio.ByteBuffer;

public class DataTypeUtils {
    public static DataType<?> createInstance(@NonNull DataType<?> source, long size, int... params) {
        if (source instanceof DecimalType) {
            Integer scale = null;
            Integer prec = null;
            if (params != null) {
                if (params.length >= 1) {
                    scale = params[0];
                }
                if (params.length >= 2) {
                    prec = params[1];
                }
            }
            return new DecimalType<>((DecimalType<?>) source, scale, prec);
        } else if (source instanceof IntegerType) {
            Integer min = null;
            Integer max = null;
            if (params != null) {
                if (params.length > 1) {
                    min = params[0];
                }
                if (params.length > 2) {
                    max = params[1];
                }
            }
            return new IntegerType((IntegerType) source, min, max);
        } else if (size > 0) {
            if (source instanceof TextType) {
                return new TextType((TextType) source, size);
            } else if (source instanceof BinaryType) {
                return new BinaryType((BinaryType) source, size);
            }
        }
        return source;
    }

    public static boolean isCompatible(@NonNull DataType<?> target,
                                       @NonNull DataType<?> current) {
        if (current.equals(target)) return true;
        if (current.getJavaType().equals(String.class)) {
            Class<?> type = target.getJavaType();
            if (ReflectionUtils.isPrimitiveTypeOrString(type)) {
                return true;
            } else if (type.equals(ByteBuffer.class)) {
                return true;
            }
        } else if (ReflectionUtils.isNumericType(current.getJavaType()) &&
                ReflectionUtils.isNumericType(target.getJavaType())) {
            return true;
        }
        return false;
    }

    public static boolean isNullType(@NonNull SchemaField field) {
        return field.getName().compareToIgnoreCase("null") == 0 ||
                field.getDataType().getJavaType().equals(ObjectUtils.Null.class);
    }
}
