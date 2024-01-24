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

package io.zyient.core.mapping.readers;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.settings.ReaderSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public abstract class InputReaderConfig {
    public static final String __CONFIG_PATH = "reader";

    private final SourceTypes[] supportedTypes;
    private final Class<? extends ReaderSettings> settingsType;
    private ReaderSettings settings;

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

    public InputReaderConfig configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, null, settingsType);
            reader.read();
            settings = (ReaderSettings) reader.settings();
            configureReader(reader.config());
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    protected abstract void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception;

    public abstract InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception;
}
