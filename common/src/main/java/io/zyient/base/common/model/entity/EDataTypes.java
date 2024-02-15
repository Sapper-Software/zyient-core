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

package io.zyient.base.common.model.entity;

import lombok.NonNull;

public enum EDataTypes {
    String,
    Boolean,
    Short,
    Integer,
    Long,
    Float,
    Double,
    Date,
    Bytes,
    Json,
    DateTime,
    Timestamp,
    Currency;

    public static Class<?> asJavaType(@NonNull EDataTypes dataType) throws Exception {
        switch (dataType) {
            case Date, DateTime -> {
                return java.util.Date.class;
            }
            case Long, Timestamp -> {
                return java.lang.Long.class;
            }
            case Bytes -> {
                return Byte.class;
            }
            case Float -> {
                return java.lang.Float.class;
            }
            case Short -> {
                return java.lang.Short.class;
            }
            case Boolean -> {
                return java.lang.Boolean.class;
            }
            case Double -> {
                return java.lang.Double.class;
            }
            case String -> {
                return java.lang.String.class;
            }
            case Integer -> {
                return java.lang.Integer.class;
            }
        }
        throw new Exception("Unknown data type.");
    }
}
