package io.zyient.base.common.utils.beans;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Inherited
public @interface TypeRefs {
    TypeRef[] refs();
}
