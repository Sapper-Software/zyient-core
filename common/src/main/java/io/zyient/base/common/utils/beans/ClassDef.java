package io.zyient.base.common.utils.beans;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

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
public class ClassDef {
    private String name;
    private Class<?> type;
    private Map<String, PropertyDef> properties;
    private Boolean abstractType = null;
    private Boolean generic = null;
    private List<Method> methods;

    public ClassDef from(@NonNull Class<?> clazz) throws Exception {
        name = clazz.getSimpleName();
        type = clazz;
        abstractType = Modifier.isAbstract(clazz.getModifiers());
        generic = ReflectionHelper.isGeneric(clazz);
        methods = new ArrayList<>();
        ReflectionHelper.getAllMethods(clazz, methods);

        Field[] fields = ReflectionHelper.getAllFields(clazz);
        if (fields != null) {
            properties = new HashMap<>();
            for (Field field : fields) {
                if (ReflectionHelper.isPrimitiveTypeOrString(field)) {
                    PropertyDef pd = new PropertyDef(field.getType());
                    pd.name(field.getName());
                    pd.owner(clazz);
                    pd.generic(false);
                    pd.setter(findSetter(field));
                    pd.getter(findGetter(field));
                    pd.accessible(Modifier.isPublic(field.getModifiers()));
                    properties.put(pd.name(), pd);
                } else if (ReflectionHelper.implementsInterface(List.class, field.getType())) {
                    ListPropertyDef pd = (ListPropertyDef) new ListPropertyDef()
                            .owner(clazz);
                    pd.from(field);
                    pd.setter(findSetter(field));
                    pd.getter(findGetter(field));
                    pd.accessible(Modifier.isPublic(field.getModifiers()));
                    properties.put(pd.name(), pd);
                } else if (ReflectionHelper.isMap(field)) {

                } else {

                }
            }
        }
        return this;
    }

    public Method findSetter(@NonNull Field field) {
        Preconditions.checkNotNull(methods);
        String name = field.getName();
        Class<?> type = field.getType();
        for (Method method : methods) {
            if (isValidSetter(name, method)) {
                Parameter[] params = method.getParameters();
                if (params != null && params.length == 1) {
                    Parameter p = params[0];
                    Class<?> pt = p.getType();
                    if (type.isAssignableFrom(pt)) {
                        if (Modifier.isPublic(method.getModifiers()))
                            return method;
                    }
                }
            }
        }
        return null;
    }

    public Method findGetter(@NonNull Field field) {
        Preconditions.checkNotNull(methods);
        String name = field.getName();
        Class<?> type = field.getType();
        for (Method method : methods) {
            if (isValidGetter(name, method)) {
                Class<?> rt = method.getReturnType();
                if (type.isAssignableFrom(rt)) {
                    if (Modifier.isPublic(method.getModifiers()))
                        return method;
                }
            }
        }
        return null;
    }

    private boolean isValidGetter(String field, Method method) {
        String name = String.format("get%s", StringUtils.capitalize(field));
        if (method.getName().compareTo(name) == 0) {
            return true;
        }
        name = String.format("is%s", StringUtils.capitalize(field));
        if (method.getName().compareTo(name) == 0) {
            return true;
        }
        return field.compareTo(method.getName()) == 0;
    }

    private boolean isValidSetter(String field, Method method) {
        String name = String.format("set%s", StringUtils.capitalize(field));
        if (method.getName().compareTo(name) == 0) {
            return true;
        }
        return field.compareTo(method.getName()) == 0;
    }
}
