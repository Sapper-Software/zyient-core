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

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.entity.PropertyBag;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class PropertyFieldDef extends PropertyDef {
    public static final String METHOD_SET_PROPERTY = "setProperty";
    public static final String METHOD_GET_PROPERTY = "getProperty";

    private Method valueSetter;
    private Method valueGetter;

    public PropertyFieldDef() {
        name(PropertyBag.FIELD_KEY);
    }

    public PropertyFieldDef(@NonNull PropertyFieldDef source) {
        super(source);
        valueSetter = source.valueSetter;
        valueGetter = source.valueGetter;
    }

    protected PropertyFieldDef init(@NonNull ClassDef parent) {
        type(Map.class);
        name("properties");

        setter(parent.findSetter(PropertyBag.FIELD_KEY, Map.class));
        Preconditions.checkNotNull(setter());
        getter(parent.findGetter(PropertyBag.FIELD_KEY, Map.class));
        Preconditions.checkNotNull(getter());
        valueGetter = findGetValue(parent.methods());
        Preconditions.checkNotNull(valueGetter);
        valueSetter = findSetValue(parent.methods());
        Preconditions.checkNotNull(valueSetter);
        return this;
    }

    @Override
    protected PropertyDef findField(@NonNull String[] parts, int index) {
        if (index == parts.length - 1) {
            List<String> keys = ReflectionHelper.extractKeys(parts[index]);
            if (keys != null && !keys.isEmpty()) {
                return this;
            }
        }
        return null;
    }

    private Method findGetValue(List<Method> methods) {
        for (Method method : methods) {
            if (method.getName().compareTo(METHOD_GET_PROPERTY) == 0) {
                Parameter[] params = method.getParameters();
                if (params != null && params.length == 1) {
                    Parameter p = params[0];
                    Class<?> pt = p.getType();
                    if (pt.equals(String.class)) {
                        if (Modifier.isPublic(method.getModifiers()))
                            return method;
                    }
                }
            }
        }
        return null;
    }

    private Method findSetValue(List<Method> methods) {
        for (Method method : methods) {
            if (method.getName().compareTo(METHOD_SET_PROPERTY) == 0) {
                Parameter[] params = method.getParameters();
                if (params != null && params.length == 2) {
                    if (Modifier.isPublic(method.getModifiers())) {
                        Parameter kp = params[0];
                        Class<?> kt = kp.getType();
                        if (kt.equals(String.class)) {
                            Parameter vp = params[1];
                            Class<?> vt = vp.getType();
                            if (vt.equals(Object.class)) {
                                return method;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }
}
