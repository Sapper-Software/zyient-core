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

package io.zyient.base.core.mapping.readers;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.mapping.SourceTypes;
import io.zyient.base.core.mapping.model.Column;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class InputReaderConfig {
    public static final String __CONFIG_PATH = "reader";
    public static final String __CONFIG_PATH_COLUMNS = "columns";
    public static final String __CONFIG_PATH_COLUMN = "column";

    private final SourceTypes[] supportedTypes;
    private final Class<? extends ReaderSettings> settingsType;
    private ReaderSettings settings;
    private final Map<String, Column> columns = new HashMap<>();

    public InputReaderConfig(SourceTypes @NonNull [] supportedTypes,
                             @NonNull Class<? extends ReaderSettings> settingsType) {
        this.supportedTypes = supportedTypes;
        this.settingsType = settingsType;
    }


    public boolean supports(@NonNull SourceTypes fileType) {
        for (SourceTypes t : supportedTypes) {
            if (t == fileType) {
                return true;
            }
        }
        return false;
    }

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    @SuppressWarnings("unchecked")
    private void readColumns(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH_COLUMNS);
        List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(__CONFIG_PATH_COLUMN);
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            Class<? extends Column> type = (Class<? extends Column>) ConfigReader.readType(node);
            if (type == null) {
                type = Column.class;
            }
            Column column = ConfigReader.read(node, type);
            column.validate();
            columns.put(column.getName(), column);
        }
    }

    public InputReaderConfig configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, null, settingsType);
            reader.read();
            settings = (ReaderSettings) reader.settings();
            readColumns(reader.config());
            configureReader(reader.config());

            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    protected abstract void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception;

    public abstract InputReader createInstance() throws Exception;
}
