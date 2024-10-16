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

package io.zyient.base.common.utils.beans;

import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class MapPropertyDef extends PropertyDef {
    public static final String REF_NAME_KEY = "mapKey";
    public static final String REF_NAME_VALUE = "mapValue";

    private PropertyDef keyType;
    private PropertyDef valueType;
    private Class<? extends Map> initType = HashMap.class;
    private List<Method> methods;

    @Override
    public boolean canInitialize() {
        if (keyType != null && valueType != null) {
            if (!ReflectionHelper.isPrimitiveTypeOrString(keyType.type())) {
                return false;
            }
            return valueType.canInitialize();
        }
        return false;
    }

    public MapPropertyDef from(@NonNull Field field,
                               @NonNull Class<?> owner) throws Exception {
        super.from(field, owner);
        generic(true);
        methods = new ArrayList<>();
        ReflectionHelper.getAllMethods(type(), methods);
        if (field.isAnnotationPresent(TypeRefs.class)) {
            TypeRefs refs = field.getAnnotation(TypeRefs.class);
            if (refs.refs() == null || refs.refs().length != 2) {
                throw new Exception(String.format("Invalid Map TypeRefs: [field=%s]", field.getName()));
            }
            TypeRef keyRef = findRef(REF_NAME_KEY, refs);
            if (keyRef == null) {
                throw new Exception(String.format("Invalid Map TypeRefs: missing ref [%s]. [field=%s]",
                        REF_NAME_KEY, field.getName()));
            }
            updateKeyType(keyRef.type());
            TypeRef valueRef = findRef(REF_NAME_VALUE, refs);
            if (valueRef == null) {
                throw new Exception(String.format("Invalid Map TypeRefs: missing ref [%s]. [field=%s]",
                        REF_NAME_VALUE, field.getName()));
            }
            updateValueType(valueRef.type());
        } else {
            updateKeyType(Object.class);
            updateValueType(Object.class);
        }
        valueType.getter(findGet());
        valueType.setter(findPut());

        return this;
    }

    private void updateKeyType(Class<?> type) throws Exception {
        if (ReflectionHelper.isPrimitiveTypeOrString(type)) {
            keyType = new PrimitivePropertyDef(type);
        } else if (!type.equals(Object.class)) {
            ClassPropertyDef def = (ClassPropertyDef) new ClassPropertyDef()
                    .owner(owner());
            def.from("__map_key", type, owner());
            keyType = def;
        } else if (type.isEnum()) {
            keyType = new EnumPropertyDef(type);
        } else {
            keyType = new UnknownPropertyDef();
        }
        keyType.name("__map_key");
        keyType.accessible(false);
    }

    private void updateValueType(Class<?> type) throws Exception {
        if (ReflectionHelper.isPrimitiveTypeOrString(type)) {
            valueType = new PrimitivePropertyDef(type);
        } else if (type.isEnum()) {
            valueType = new EnumPropertyDef(type);
        } else if (!type.equals(Object.class)) {
            ClassPropertyDef def = (ClassPropertyDef) new ClassPropertyDef()
                    .owner(owner());
            def.from("__map_value", type, owner());
            valueType = def;
        } else {
            valueType = new UnknownPropertyDef();
        }
        valueType.name("__map_value");
        valueType.accessible(false);
    }

    private TypeRef findRef(String name, TypeRefs refs) {
        for (TypeRef ref : refs.refs()) {
            if (name.compareTo(ref.value()) == 0) {
                return ref;
            }
        }
        return null;
    }

    private Method findPut() {
        for (Method method : methods) {
            if (method.getName().compareTo("put") == 0) {
                Parameter[] params = method.getParameters();
                if (params != null && params.length == 2) {
                    Parameter p = params[0];
                    Class<?> pt = p.getType();
                    boolean keyMatch = false;
                    if (pt.equals(Object.class) || keyType.type().isAssignableFrom(pt)) {
                        if (Modifier.isPublic(method.getModifiers())) {
                            keyMatch = true;
                        }
                    }
                    if (keyMatch) {
                        p = params[1];
                        pt = p.getType();
                        if (pt.equals(Object.class) || valueType.type().isAssignableFrom(pt)) {
                            return method;
                        }
                    }
                }
            }
        }
        return null;
    }

    private Method findGet() {
        for (Method method : methods) {
            if (method.getName().compareTo("get") == 0) {
                Parameter[] params = method.getParameters();
                if (params != null && params.length == 1) {
                    Parameter p = params[0];
                    Class<?> pt = p.getType();
                    if (pt.equals(Object.class) || keyType.type().isAssignableFrom(pt)) {
                        if (Modifier.isPublic(method.getModifiers())) {
                            return method;
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    protected PropertyDef findField(@NonNull String[] parts, int index) {
        String name = parts[index];
        if (index == parts.length - 1) {
            if (name.compareTo(name()) == 0) {
                return this;
            }
        } else {
            if (valueType instanceof ClassPropertyDef) {
                return valueType.findField(parts, index + 1);
            }
        }
        return null;
    }
}
