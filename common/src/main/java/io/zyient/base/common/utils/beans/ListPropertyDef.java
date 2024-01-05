package io.zyient.base.common.utils.beans;

import com.google.common.base.Preconditions;
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
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
@SuppressWarnings("rawtypes")
public class ListPropertyDef extends PropertyDef {
    private PropertyDef innerType;
    private Class<? extends List> initType = ArrayList.class;
    private List<Method> methods;

    public ListPropertyDef() {
    }

    public ListPropertyDef(@NonNull ListPropertyDef source) {
        super(source);
        innerType = source.innerType;
        initType = source.initType;
        methods = source.methods;
    }

    @Override
    public boolean canInitialize() {
        if (innerType != null) {
            return innerType.canInitialize();
        }
        return false;
    }

    public ListPropertyDef from(@NonNull Field field) throws Exception {
        Preconditions.checkArgument(ReflectionHelper.implementsInterface(List.class, field.getType()));
        name(field.getName());
        type(field.getType());
        abstractType(Modifier.isAbstract(type().getModifiers()));
        generic(true);
        methods = new ArrayList<>();
        ReflectionHelper.getAllMethods(type(), methods);

        if (field.isAnnotationPresent(TypeRef.class)) {
            TypeRef ref = field.getAnnotation(TypeRef.class);
            updateInnerType(ref.type());
        } else {
            updateInnerType(Object.class);
        }
        return this;
    }

    private void updateInnerType(Class<?> type) throws Exception {
        if (ReflectionHelper.isPrimitiveTypeOrString(type)) {
            innerType = new PropertyDef(type);
            innerType.accessible(false);
        } else if (!type.equals(Object.class)) {
            ClassPropertyDef def  = (ClassPropertyDef) new ClassPropertyDef()
                    .owner(owner());
            def.from("__list_inner", type);
            innerType = def;
        } else {
            innerType = new PropertyDef();
            innerType.type(Object.class);
        }
        innerType.name("__list_inner");
        innerType.generic(true);
        innerType.getter(findGet());
        innerType.setter(findAdd());
    }

    private Method findAdd() {
        for (Method method : methods) {
            if (method.getName().compareTo("add") == 0) {
                Parameter[] params = method.getParameters();
                if (params != null && params.length == 1) {
                    Parameter p = params[0];
                    Class<?> pt = p.getType();
                    if (pt.equals(Object.class) || innerType.type().isAssignableFrom(pt)) {
                        if (Modifier.isPublic(method.getModifiers())) {
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
                    if (pt.equals(Integer.class)) {
                        if (Modifier.isPublic(method.getModifiers())) {
                            return method;
                        }
                    }
                }
            }
        }
        return null;
    }
}
