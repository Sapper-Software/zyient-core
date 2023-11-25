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

package io.zyient.base.core.stores.impl.solr.schema;

import io.zyient.base.common.utils.ReflectionUtils;
import lombok.NonNull;

import java.lang.reflect.Field;

public enum SolrFieldTypes {
    Binary("binary"),
    Boolean("boolean"),
    String("string"),
    Date("pdate"),
    Int("pint"),
    Long("plong"),
    Float("pfloat"),
    Double("pdouble"),
    IntArray("pints"),
    LongArray("plongs"),
    FloatArray("pfloats"),
    DoubleArray("pdoubles"),
    StringArray("strings"),
    DateArray("pdates"),
    Text("text_");

    private final String type;

    SolrFieldTypes(@NonNull String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }

    public static SolrFieldTypes getType(@NonNull Field field) throws Exception {
        Class<?> t = field.getType();
        if (ReflectionUtils.isCollection(t)) {
            Class<?> inner = ReflectionUtils.getGenericCollectionType(field);
            return getArrayType(getType(inner));
        } else if (t.isArray()) {
            Class<?> inner = t.getComponentType();
            return getArrayType(getType(inner));
        }
        return getType(t);
    }

    private static SolrFieldTypes getArrayType(SolrFieldTypes t) {
        switch (t) {
            case Int -> {
                return IntArray;
            }
            case Long -> {
                return LongArray;
            }
            case Float -> {
                return FloatArray;
            }
            case Double -> {
                return DoubleArray;
            }
            case Date -> {
                return DateArray;
            }
            case String -> {
                return StringArray;
            }
        }
        return String;
    }

    public static SolrFieldTypes getType(@NonNull Class<?> t) throws Exception {
        if (ReflectionUtils.isPrimitiveTypeOrString(t)) {
            if (ReflectionUtils.isShort(t) || ReflectionUtils.isInt(t)) {
                return Int;
            } else if (ReflectionUtils.isLong(t)) {
                return Long;
            } else if (ReflectionUtils.isFloat(t)) {
                return Float;
            } else if (ReflectionUtils.isDouble(t)) {
                return Double;
            } else if (ReflectionUtils.isBoolean(t)) {
                return Boolean;
            } else if (t.equals(java.lang.String.class)) {
                return String;
            }
        } else if (t.equals(java.util.Date.class)) {
            return Date;
        }
        return String;
    }

}