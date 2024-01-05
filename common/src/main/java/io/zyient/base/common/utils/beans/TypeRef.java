package io.zyient.base.common.utils.beans;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Inherited
public @interface TypeRef {
    String value() default "";
    Class<?> type();
}
