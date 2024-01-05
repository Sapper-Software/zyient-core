package io.zyient.base.common.utils.beans;

import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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

    public MapPropertyDef from(@NonNull Field field) throws Exception {
        name(field.getName());
        type(field.getType());
        abstractType(Modifier.isAbstract(type().getModifiers()));
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
        }
        return this;
    }

    private void updateKeyType(Class<?> type) throws Exception {
        if (ReflectionHelper.isPrimitiveTypeOrString(type)) {
            keyType = new PropertyDef(type);
            keyType.accessible(false);
        } else if (!type.equals(Object.class)) {
            ClassPropertyDef def = (ClassPropertyDef) new ClassPropertyDef()
                    .owner(owner());
            def.from("__list_inner", type);
            keyType = def;
        } else {
            keyType = new PropertyDef();
            keyType.type(Object.class);
        }
        keyType.name("__list_inner");
        keyType.generic(true);
    }

    private TypeRef findRef(String name, TypeRefs refs) {
        for (TypeRef ref : refs.refs()) {
            if (name.compareTo(ref.value()) == 0) {
                return ref;
            }
        }
        return null;
    }
}
