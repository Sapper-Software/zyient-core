package io.zyient.base.common.utils.beans;

import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.reflect.ConstructorUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

@Getter
@Setter
@Accessors(fluent = true)
public class PropertyDef {
    private Class<?> owner = null;
    private String name;
    private String path = null;
    private Class<?> type;
    private Method getter;
    private Method setter;
    private Boolean generic = false;
    private Boolean abstractType = null;
    private Boolean accessible = null;

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
    }

    public boolean canInitialize() {
        if (type.equals(Object.class)) return false;
        if (abstractType == null) {
            abstractType = Modifier.isAbstract(type.getModifiers());
        }
        if (!abstractType) {
            if (ReflectionHelper.isPrimitiveTypeOrString(type)) {
                return true;
            } else {
                Constructor<?> ctor = ConstructorUtils.getAccessibleConstructor(type);
                return (ctor != null);
            }
        }
        return false;
    }
}
