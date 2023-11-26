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

package io.zyient.cdc.entity.utils;

import io.zyient.base.common.utils.ReflectionHelper;
import lombok.NonNull;

public class ConversionUtils {
    public static Integer getInt(@NonNull Object data) throws Exception {
        if (ReflectionHelper.isNumericType(data.getClass())) {
            if (data instanceof Integer) {
                return (Integer) data;
            } else if (data instanceof Short) {
                return ((Short) data).intValue();
            } else if (data instanceof Long) {
                return ((Long) data).intValue();
            } else if (data instanceof Float) {
                return ((Float) data).intValue();
            } else if (data instanceof Double) {
                return ((Double) data).intValue();
            }
        }
        return null;
    }

    public static Short getShort(@NonNull Object data) throws Exception {
        if (ReflectionHelper.isNumericType(data.getClass())) {
            if (data instanceof Short) {
                return (Short) data;
            } else if (data instanceof Integer) {
                return ((Integer) data).shortValue();
            } else if (data instanceof Long) {
                return ((Long) data).shortValue();
            } else if (data instanceof Float) {
                return ((Float) data).shortValue();
            } else if (data instanceof Double) {
                return ((Double) data).shortValue();
            }
        }
        return null;
    }

    public static Long getLong(@NonNull Object data) throws Exception {
        if (ReflectionHelper.isNumericType(data.getClass())) {
            if (data instanceof Long) {
                return (Long) data;
            } else if (data instanceof Integer) {
                return ((Integer) data).longValue();
            } else if (data instanceof Short) {
                return ((Short) data).longValue();
            } else if (data instanceof Float) {
                return ((Float) data).longValue();
            } else if (data instanceof Double) {
                return ((Double) data).longValue();
            }
        }
        return null;
    }

    public static Float getFloat(@NonNull Object data) throws Exception {
        if (ReflectionHelper.isNumericType(data.getClass())) {
            if (data instanceof Float) {
                return (Float) data;
            } else if (data instanceof Integer) {
                return ((Integer) data).floatValue();
            } else if (data instanceof Short) {
                return ((Short) data).floatValue();
            } else if (data instanceof Long) {
                return ((Long) data).floatValue();
            } else if (data instanceof Double) {
                return ((Double) data).floatValue();
            }
        }
        return null;
    }

    public static Double getDouble(@NonNull Object data) throws Exception {
        if (ReflectionHelper.isNumericType(data.getClass())) {
            if (data instanceof Double) {
                return (Double) data;
            } else if (data instanceof Integer) {
                return ((Integer) data).doubleValue();
            } else if (data instanceof Short) {
                return ((Short) data).doubleValue();
            } else if (data instanceof Long) {
                return ((Long) data).doubleValue();
            } else if (data instanceof Float) {
                return ((Float) data).doubleValue();
            }
        }
        return null;
    }

}
