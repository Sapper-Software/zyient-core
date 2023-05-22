package ai.sapper.cdc.entity.utils;

import ai.sapper.cdc.common.utils.ReflectionUtils;
import lombok.NonNull;

public class ConversionUtils {
    public static Integer getInt(@NonNull Object data) throws Exception {
        if (ReflectionUtils.isNumericType(data.getClass())) {
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
        if (ReflectionUtils.isNumericType(data.getClass())) {
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
        if (ReflectionUtils.isNumericType(data.getClass())) {
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
        if (ReflectionUtils.isNumericType(data.getClass())) {
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
        if (ReflectionUtils.isNumericType(data.getClass())) {
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
