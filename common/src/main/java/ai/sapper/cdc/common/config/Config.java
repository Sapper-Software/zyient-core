package ai.sapper.cdc.common.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Config {
    String name();

    boolean required() default true;

    Class<?> type() default String.class;

    boolean autoUpdate() default false;

    Class<? extends ConfigValueParser<?>> parser() default ConfigValueParser.DummyValueParser.class;
}
