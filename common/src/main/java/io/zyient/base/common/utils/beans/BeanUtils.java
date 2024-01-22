package io.zyient.base.common.utils.beans;

import com.google.common.base.Strings;
import io.zyient.base.common.cache.LRUCache;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class BeanUtils {
    private static final ReentrantLock __cacheLock = new ReentrantLock();
    private static final LRUCache<Class<?>, ClassDef> __classDefs = new LRUCache<>(1024);

    public static ClassDef get(@NonNull Class<?> clazz) throws Exception {
        __cacheLock.lock();
        try {
            if (__classDefs.containsKey(clazz)) {
                Optional<ClassDef> o = __classDefs.get(clazz);
                if (o.isPresent()) return o.get();
            }
            ClassDef def = new ClassDef();
            def.name(clazz.getSimpleName());
            def.type(clazz);
            __classDefs.put(def.type(), def);
            def.from(clazz);
            return def;
        } finally {
            __cacheLock.unlock();
        }
    }

    public enum FieldType {
        Field, List, Map
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class FieldDef {
        private String name;
        private FieldType type = FieldType.Field;
        private String index;

        public static FieldDef parse(@NonNull String name) {
            FieldDef def = new FieldDef();
            if (name.contains("[")) {
                def.type = FieldType.List;
                int s = name.indexOf('[');
                int e = name.indexOf(']');
                def.name = name.substring(0, s);
                def.index = name.substring(s + 1, e);
            } else if (name.contains("(")) {
                def.type = FieldType.Map;
                int s = name.indexOf('(');
                int e = name.indexOf(')');
                def.name = name.substring(0, s);
                def.index = name.substring(s + 1, e);
            } else {
                def.name = name;
            }
            return def;
        }
    }

    public static Object getValue(@NonNull Object source,
                                  @NonNull String path) throws Exception {
        ClassDef def = get(source.getClass());
        FieldBuilder builder = new FieldBuilder(path);
        PropertyDef property = null;
        Object value = source;
        do {
            FieldDef fd = builder.next();
            if (fd == null) {
                return value;
            }
            property = def.get(fd.name);
            if (property == null) {
                throw new Exception(String.format("Property not found. [type=%s][property=%s]",
                        def.type().getCanonicalName(), fd.name));
            }
            value = getValue(value, fd, property);
            if (value != null && !ReflectionHelper.isPrimitiveTypeOrString(value.getClass())) {
                def = get(value.getClass());
            }
        } while (value != null);
        return null;
    }

    public static void setValue(@NonNull Object target,
                                @NonNull String path,
                                Object value) throws Exception {
        ClassDef def = get(target.getClass());
        FieldBuilder builder = new FieldBuilder(path);
        Object current = target;
        while (true) {
            FieldDef fd = builder.next();
            PropertyDef property = def.get(fd.name);
            if (property == null) {
                throw new Exception(String.format("Property not found. [type=%s][property=%s]",
                        def.type().getCanonicalName(), fd.name));
            }
            if (!builder.hasNext()) {
                setValue(current, fd, property, value);
                break;
            } else {
                current = setValue(current, fd, property, null);
                def = get(current.getClass());
            }
        }
    }

    public static Object setValueFromString(@NonNull Object target,
                                            @NonNull String path,
                                            @NonNull String value) throws Exception {
        ClassDef def = get(target.getClass());
        FieldBuilder builder = new FieldBuilder(path);
        Object current = target;
        while (true) {
            FieldDef fd = builder.next();
            PropertyDef property = def.get(fd.name);
            if (property == null) {
                throw new Exception(String.format("Property not found. [type=%s][property=%s]",
                        def.type().getCanonicalName(), fd.name));
            }
            if (!builder.hasNext()) {
                Object v = ReflectionHelper.parseStringValue(property.type(), value);
                setValue(current, fd, property, v);
                return v;
            } else {
                current = setValue(current, fd, property, null);
                def = get(current.getClass());
            }
        }
    }

    private static Object setValue(Object target, FieldDef fd, PropertyDef pd, Object value) throws Exception {
        Method setter = pd.setter();
        if (setter == null) {
            throw new Exception(String.format("No valid setter for property. [type=%s][property=%s]",
                    target.getClass().getCanonicalName(), pd.name()));
        }
        if (fd.type == FieldType.Field) {
            if (value == null) {
                value = getValue(target, fd, pd);
                if (value == null) {
                    if (pd.canInitialize()) {
                        value = pd.type()
                                .getDeclaredConstructor()
                                .newInstance();
                    } else {
                        throw new Exception(String.format("Cannot auto-initialize value of type. [type=%s]",
                                pd.type().getCanonicalName()));
                    }
                } else {
                    return value;
                }
            }
            setter.invoke(target, value);
        } else if (fd.type == FieldType.List) {
            if (!(pd instanceof ListPropertyDef)) {
                throw new Exception(String.format("Expected list property. [type=%s]",
                        pd.getClass().getCanonicalName()));
            }
            ListPropertyDef lpd = (ListPropertyDef) pd;
            List<?> values = null;
            if (value instanceof List<?>) {
                setter.invoke(target, value);
                return value;
            } else {
                values = initList(lpd, target);
                if (value == null) {
                    if (!Strings.isNullOrEmpty(fd.index)) {
                        int index = Integer.parseInt(fd.index);
                        if (index < 0 || index > values.size()) {
                            throw new ArrayIndexOutOfBoundsException(index);
                        }
                        if (index < values.size())
                            value = lpd.innerType().getter().invoke(values, index);
                        else {
                            value = lpd.innerType()
                                    .type().getDeclaredConstructor()
                                    .newInstance();
                            lpd.innerType().setter().invoke(values, value);
                        }
                    } else {
                        throw new Exception("List index not specified...");
                    }
                } else
                    lpd.innerType().setter().invoke(values, value);
            }
        } else if (fd.type == FieldType.Map) {
            if (!(pd instanceof MapPropertyDef)) {
                throw new Exception(String.format("Expected map property. [type=%s]",
                        pd.getClass().getCanonicalName()));
            }
            MapPropertyDef mpd = (MapPropertyDef) pd;
            Map<?, ?> map = null;
            if (value instanceof Map<?, ?>) {
                setter.invoke(target, value);
            } else {
                map = initMap(mpd, target);
                Object key = null;
                if (!Strings.isNullOrEmpty(fd.index)) {
                    key = ReflectionHelper.as(mpd.keyType().type(), fd.index);
                    if (key == null) {
                        throw new Exception("Map key cannot be null...");
                    }
                } else {
                    throw new Exception("Map key not specified...");
                }
                if (value == null) {
                    value = mpd.valueType().getter().invoke(map, key);
                    if (value == null) {
                        value = mpd.valueType().type()
                                .getDeclaredConstructor()
                                .newInstance();
                        mpd.valueType().setter().invoke(map, key, value);
                    }
                } else {
                    mpd.valueType().setter().invoke(map, key, value);
                }
            }
        }
        return value;
    }

    private static Map<?, ?> initMap(MapPropertyDef pd, Object target) throws Exception {
        Map<?, ?> map = (Map<?, ?>) pd.getter().invoke(target);
        if (map == null) {
           if (pd.canInitialize()) {
               map = pd.initType()
                       .getDeclaredConstructor()
                       .newInstance();
               pd.setter().invoke(target, map);
           } else {
               throw new Exception(String.format("Cannot auto-initialize value of type. [key=%s][value=%s]",
                       pd.keyType().type().getCanonicalName(), pd.valueType().type().getCanonicalName()));
           }
        }
        return map;
    }

    private static List<?> initList(ListPropertyDef pd, Object target) throws Exception {
        List<?> values = (List<?>) pd.getter().invoke(target);
        if (values == null) {
            if (pd.canInitialize()) {
                values = pd.initType()
                        .getDeclaredConstructor()
                        .newInstance();
                pd.setter().invoke(target, values);
            } else {
                throw new Exception(String.format("Cannot auto-initialize value of type. [type=%s]",
                        pd.innerType().type().getCanonicalName()));
            }
        }
        return values;
    }

    private static Object getValue(Object source, FieldDef fd, PropertyDef pd) throws Exception {
        Method getter = pd.getter();
        if (getter == null) {
            throw new Exception(String.format("No valid getter for property. [type=%s][property=%s]",
                    source.getClass().getCanonicalName(), pd.name()));
        }
        if (fd.type == FieldType.Field) {
            return getter.invoke(source);
        } else if (fd.type == FieldType.List) {
            if (!(pd instanceof ListPropertyDef)) {
                throw new Exception(String.format("Expected list property. [type=%s]",
                        pd.getClass().getCanonicalName()));
            }
            PropertyDef ipd = ((ListPropertyDef) pd).innerType();
            if (ipd.getter() != null) {
                List<?> values = (List<?>) getter.invoke(source);
                if (values != null) {
                    int index = Integer.parseInt(fd.index);
                    if (index < values.size()) {
                        return ipd.getter().invoke(values, index);
                    }
                }
            } else {
                throw new Exception(String.format("No getter defined. [field=%s]", pd.name()));
            }
        } else if (fd.type == FieldType.Map) {
            if (!(pd instanceof MapPropertyDef)) {
                throw new Exception(String.format("Expected map property. [type=%s]",
                        pd.getClass().getCanonicalName()));
            }
            PropertyDef vpd = ((MapPropertyDef) pd).valueType();
            if (vpd.getter() != null) {
                Map<?, ?> map = (Map<?, ?>) getter.invoke(source);
                if (map != null) {
                    Object key = getMapKey(fd.index, (MapPropertyDef) pd);
                    return vpd.getter().invoke(map, key);
                }
            } else {
                throw new Exception(String.format("No getter defined. [field=%s]", pd.name()));
            }
        }
        return null;
    }

    private static Object getMapKey(String key, MapPropertyDef def) throws Exception {
        if (!ReflectionHelper.isPrimitiveTypeOrString(def.keyType().type())) {
            throw new Exception(String.format("Key type not supported. [type=%s]",
                    def.keyType().type().getCanonicalName()));
        }
        return ReflectionHelper.parseStringValue(def.keyType().type(), key);
    }

    public static class FieldBuilder {
        private final List<FieldDef> fields = new ArrayList<>();
        private int index = 0;

        public FieldBuilder(@NonNull String path) {
            if (path.contains(".")) {
                String[] parts = path.split("\\.");
                for (String part : parts) {
                    fields.add(FieldDef.parse(part));
                }
            } else {
                fields.add(FieldDef.parse(path));
            }
        }

        public FieldDef next() {
            if (index < fields.size()) {
                FieldDef def = fields.get(index);
                index++;
                return def;
            }
            return null;
        }

        public boolean hasNext() {
            return (index < fields.size());
        }
    }
}
