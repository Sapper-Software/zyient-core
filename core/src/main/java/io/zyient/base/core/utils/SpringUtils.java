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
