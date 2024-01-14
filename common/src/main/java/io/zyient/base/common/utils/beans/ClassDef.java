package io.zyient.base.common.utils.beans;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.PropertyBag;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.*;
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
    private Map<String, PropertyDef> properties = new HashMap<>();
    private Boolean abstractType = null;
    private Boolean generic = null;
    private List<Method> methods;
    private List<Constructor<?>> constructors;
    private Constructor<?> emptyConstructor;

    public PropertyDef get(@NonNull String name) {
        if (properties != null) {
            return properties.get(name);
        }
        return null;
    }

    public PropertyDef findField(@NonNull String name) {
        if (name.contains(".")) {
            String[] parts = name.split("\\.");
            if (properties.containsKey(parts[0])) {
                return properties.get(parts[0]).findField(parts, 1);
            }
        } else if (properties.containsKey(name)) {
            return properties.get(name);
        }
        return null;
    }

    public ClassDef from(@NonNull Class<?> clazz) throws Exception {
        Preconditions.checkNotNull(type);
        Preconditions.checkState(!Strings.isNullOrEmpty(name));

        abstractType = Modifier.isAbstract(clazz.getModifiers());
        generic = ReflectionHelper.isGeneric(clazz);
        methods = new ArrayList<>();
        ReflectionHelper.getAllMethods(clazz, methods);
        if (!abstractType) {
            Constructor<?>[] ctors = clazz.getConstructors();
            constructors = new ArrayList<>(ctors.length);
            for (Constructor<?> ctor : ctors) {
                if (ctor.getParameters() == null || ctor.getParameters().length == 0) {
                    if (Modifier.isPublic(ctor.getModifiers())) {
                        emptyConstructor = ctor;
                    }
                }
                constructors.add(ctor);
            }
        }
        Field[] fields = ReflectionHelper.getAllFields(clazz);
        if (fields != null) {
            for (Field field : fields) {
                PropertyDef pd = null;
                if (ReflectionHelper.isPrimitiveTypeOrString(field)) {
                    pd = (PrimitivePropertyDef) new PrimitivePropertyDef(field.getType())
                            .from(field, clazz);
                    pd.generic(false);
                } else if (ReflectionHelper.implementsInterface(List.class, field.getType())) {
                    pd = (ListPropertyDef) new ListPropertyDef()
                            .from(field, clazz);
                    pd.generic(true);
                } else if (ReflectionHelper.isMap(field)) {
                    pd = (MapPropertyDef) new MapPropertyDef()
                            .from(field, clazz);
                    pd.generic(true);
                } else if (field.isEnumConstant()) {
                    pd = (EnumPropertyDef) new EnumPropertyDef()
                            .from(field, clazz);
                    pd.generic(false);
                } else {
                    pd = (ClassPropertyDef) new ClassPropertyDef()
                            .from(field, clazz);
                }
                pd.setter(findSetter(field.getName(), field.getType()));
                pd.getter(findGetter(field.getName(), field.getType()));
                properties.put(pd.name(), pd);
            }
        }
        if (ReflectionHelper.implementsInterface(PropertyBag.class, clazz)) {
           PropertyFieldDef pd = new PropertyFieldDef()
                   .init(this);
           properties.put(pd.name(), pd);
        }
        return this;
    }

    public Method findSetter(@NonNull String name,
                             @NonNull Class<?> type) {
        Preconditions.checkNotNull(methods);
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

    public Method findGetter(@NonNull String name,
                             @NonNull Class<?> type) {
        Preconditions.checkNotNull(methods);
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

    public Constructor<?> getConstructor(Parameter... parameters) {
        if (parameters == null) {
            return emptyConstructor;
        } else {
            for (Constructor<?> ctor : constructors) {
                if (checkParameters(ctor, parameters)) {
                    return ctor;
                }
            }
        }
        return null;
    }

    private boolean checkParameters(Constructor<?> ctor, Parameter... parameters) {
        Parameter[] params = ctor.getParameters();
        if (params.length == parameters.length) {
            for (int ii = 0; ii < parameters.length; ii++) {
                if (!parameters[ii].equals(params[ii])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
