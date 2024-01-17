/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.extraction.view;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.lang.reflect.Field;
import java.util.ResourceBundle;

@Getter
@Accessors(fluent = true)
public class ViewManager {
    private ResourceBundle resourceBundle;
    private ViewManagerSettings settings;

    public ViewManager configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                 @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, ViewManagerSettings.class);
            reader.read();
            settings = (ViewManagerSettings) reader.settings();
            if (!Strings.isNullOrEmpty(settings.getOverrideLocale())) {

            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    public String getDisplayText(@NonNull Field field) {
        Preconditions.checkNotNull(resourceBundle);
        String text = field.getName();
        if (field.isAnnotationPresent(ViewAttribute.class)) {
            ViewAttribute attr  = field.getAnnotation(ViewAttribute.class);
            if (!Strings.isNullOrEmpty(attr.value())) {
                text = attr.value();
                String rs = resourceBundle.getString(text);
                if (!Strings.isNullOrEmpty(rs)) {
                    text = rs;
                }
            }
        }
        return text;
    }

    public ViewType getViewType(@NonNull Field field) {
        Preconditions.checkNotNull(resourceBundle);
        ViewType type = ViewType.TEXT;
        if (field.isAnnotationPresent(ViewAttribute.class)) {
            ViewAttribute attr = field.getAnnotation(ViewAttribute.class);
            type = attr.type();
        }
        return type;
    }
}
