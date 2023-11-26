/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.common.errors.impl;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.errors.Error;
import io.zyient.base.common.errors.Errors;
import io.zyient.base.common.errors.ErrorsReader;
import io.zyient.base.common.errors.settings.XmlErrorsReaderSettings;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

@Getter
@Accessors(fluent = true)
public class XmlErrorsReader implements ErrorsReader {
    private Errors parent;
    private XmlErrorsReaderSettings settings;
    private File dir;

    @Override
    public ErrorsReader withLoader(@NonNull Errors loader) {
        parent = loader;
        return this;
    }

    @Override
    public ErrorsReader configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            Preconditions.checkNotNull(parent);
            ConfigReader r = new ConfigReader(xmlConfig, __CONFIG_PATH, XmlErrorsReaderSettings.class);
            r.read();
            settings = (XmlErrorsReaderSettings) r.settings();
            dir = new File(settings.getBaseDir());
            if (!dir.exists()) {
                throw new ConfigurationException(String.format("Base directory not found. [path=%s]",
                        dir.getAbsolutePath()));
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public List<Error> read() throws Exception {
        Preconditions.checkNotNull(settings);
        Locale locale = Locale.getDefault();
        String path = PathUtils.formatPath(String.format("%s/%s/%s",
                dir.getAbsolutePath(),
                locale.getISO3Language().toLowerCase(),
                settings.getFilename()));
        File file = new File(path);
        if (!file.exists()) {
            throw new Exception(String.format("Errors file not found. [path=%s]", file.getAbsolutePath()));
        }
        XmlMapper mapper = new XmlMapper();
        Object data = mapper.readValue(file, Object.class);
        if (data != null) {
            List<Error> errors = new ArrayList<>();
            Class<?> type = data.getClass();
            if (type.isArray()) {
                Object[] array = (Object[]) data;
                for (Object elem : array) {
                    Error error = mapper.convertValue(elem, Error.class);
                    errors.add(error);
                }
            } else if (ReflectionHelper.isCollection(type)) {
                Collection<?> array = (Collection<?>) data;
                for (Object elem : array) {
                    Error error = mapper.convertValue(elem, Error.class);
                    errors.add(error);
                }
            } else {
                Error error = mapper.convertValue(data, Error.class);
                errors.add(error);
            }
            return errors;
        }
        return null;
    }
}
