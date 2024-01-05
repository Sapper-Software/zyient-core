package io.zyient.base.core.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.web.context.support.StandardServletEnvironment;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.Set;

public class SpringUtils {
    public static Set<Class<?>> findAllEntityClassesInPackage(@NonNull String packageName,
                                                              @NonNull Class<? extends Annotation> annotation)
            throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(packageName));
        final Set<Class<?>> result = new HashSet<>();
        final ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(
                true, new StandardServletEnvironment());
        provider.addIncludeFilter(new AnnotationTypeFilter(annotation));
        for (BeanDefinition beanDefinition : provider
                .findCandidateComponents(packageName)) {
            result.add(Class.forName(beanDefinition.getBeanClassName()));
        }
        if (!result.isEmpty())
            return result;
        return null;
    }
}
