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

package io.zyient.base.core.errors.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.errors.Error;
import io.zyient.base.core.errors.Errors;
import io.zyient.base.core.errors.ErrorsReader;
import io.zyient.base.core.errors.settings.JsonErrorsReaderSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.List;
import java.util.Locale;

@Getter
@Accessors(fluent = true)
public class JsonErrorsReader implements ErrorsReader {
    private Errors parent;
    private JsonErrorsReaderSettings settings;
    private File dir;

    @Override
    public ErrorsReader withLoader(@NonNull Errors loader) {
        parent = loader;
        return this;
    }

    @Override
    public ErrorsReader configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                  @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            Preconditions.checkNotNull(parent);
            ConfigReader r = new ConfigReader(xmlConfig, __CONFIG_PATH, JsonErrorsReaderSettings.class);
            r.read();
            settings = (JsonErrorsReaderSettings) r.settings();
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
    @SuppressWarnings("unchecked")
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
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(file, List.class);
    }
}
