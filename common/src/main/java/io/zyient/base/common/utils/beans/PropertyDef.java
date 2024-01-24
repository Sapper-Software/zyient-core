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

@Getter
@Setter
@Accessors(fluent = true)
public abstract class PropertyDef {
    private Class<?> owner = null;
    private String name;
    private String path = null;
    private Class<?> type;
    private Method getter;
    private Method setter;
    private Boolean generic = false;
    private Boolean abstractType = null;
    private Boolean accessible = null;
    private Field field;

    public PropertyDef() {
    }

    public PropertyDef(@NonNull Class<?> type) {
        this.type = type;
        this.name = type.getSimpleName();
    }

    public PropertyDef(@NonNull PropertyDef source) {
        owner = source.owner;
        name = source.name;
        path = source.path;
        type = source.type;
        getter = source.getter;
        setter = source.setter;
        generic = source.generic;
        abstractType = source.abstractType;
        accessible = source.accessible;
        field = source.field;
    }

    public boolean canInitialize() {
        if (type.equals(Object.class)) return false;
        if (abstractType == null) {
            abstractType = Modifier.isAbstract(type.getModifiers());
        }
        if (!abstractType) {
            return ReflectionHelper.isPrimitiveTypeOrString(type);
        }
        return false;
    }

    public PropertyDef findField(@NonNull String name) {
        if (this.name.compareTo(name) == 0) {
            return this;
        }
        return null;
    }

    protected PropertyDef findField(@NonNull String[] parts, int index) {
        if (index == parts.length - 1) {
            String name = parts[index];
            return findField(name);
        }
        return null;
    }

    public PropertyDef from(@NonNull Field field,
                            @NonNull Class<?> owner) throws Exception {
        name = field.getName();
        type = field.getType();
        abstractType = Modifier.isAbstract(type.getModifiers());
        accessible = Modifier.isPublic(field.getModifiers());
        this.field = field;
        this.owner = owner;
        return this;
    }
}
