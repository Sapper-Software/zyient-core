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

package io.zyient.core.mapping.readers;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.content.SourceTypes;
import io.zyient.base.core.utils.FileTypeDetector;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.settings.ReaderFactorySettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class InputReaderFactory {
    public static final String __CONFIG_PATH = "readers";
    public static final String __CONFIG_PATH_FACTORY = String.format("%s.factory", __CONFIG_PATH);

    private ReaderFactorySettings settings;
    private final Map<String, InputReaderConfig> readerConfigs = new HashMap<>();
    private final Map<SourceTypes, String> sourceTypeDefaults = new HashMap<>();

    public InputReaderFactory init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH);
            if (ConfigReader.checkIfNodeExists(xmlConfig, __CONFIG_PATH_FACTORY)) {
                HierarchicalConfiguration<ImmutableNode> factoryConfig = xmlConfig.configurationAt(__CONFIG_PATH_FACTORY);
                ConfigReader reader = new ConfigReader(factoryConfig, null, ReaderFactorySettings.class);
                reader.read();
                settings = (ReaderFactorySettings) reader.settings();
            } else {
                settings = new ReaderFactorySettings();
            }
            readReaderConfigs(config);
            if (settings.getDefaults() != null && !settings.getDefaults().isEmpty()) {
                for (String key : settings.getDefaults().keySet()) {
                    SourceTypes t = SourceTypes.valueOf(key);
                    String r = settings.getDefaults().get(key);
                    if (!readerConfigs.containsKey(r)) {
                        throw new ConfigurationException(String.format("Reader not found. [name=%s]", r));
                    }
                    sourceTypeDefaults.put(t, r);
                }
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void readReaderConfigs(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        List<HierarchicalConfiguration<ImmutableNode>> rcs = config.configurationsAt(InputReaderConfig.__CONFIG_PATH);
        if (rcs == null || rcs.isEmpty()) {
            throw new Exception("No reader configurations defined...");
        }
        for (HierarchicalConfiguration<ImmutableNode> node : rcs) {
            Class<? extends InputReaderConfig> type = (Class<? extends InputReaderConfig>) ConfigReader.readType(node);
            if (type == null) {
                throw new Exception(String.format("Reader type not defined. [expected=%s]",
                        InputReaderConfig.class.getCanonicalName()));
            }
            InputReaderConfig rc = type.getDeclaredConstructor().newInstance();
            rc.configure(node);
            readerConfigs.put(rc.name(), rc);
        }
    }

    public InputReader getReader(@NonNull InputContentInfo inputContentInfo) throws Exception {
        if (inputContentInfo.path() == null)
            throw new IOException("Content path not specified...");
        if (inputContentInfo.contentType() == SourceTypes.UNKNOWN) {
            FileTypeDetector detector = new FileTypeDetector(inputContentInfo.path());
            detector.detect();
            inputContentInfo.contentType(detector.type());
        }
        InputReaderConfig config = null;
        if (Strings.isNullOrEmpty(inputContentInfo.reader())) {
            if (inputContentInfo.contentType() != SourceTypes.UNKNOWN) {
                config = findConfig(inputContentInfo);
            } else {
                throw new IOException("Failed to detect reader: name and content type is unavailable...");
            }
        } else {
            config = readerConfigs.get(inputContentInfo.reader());
        }
        if (config == null) {
            throw new IOException(String.format("Input Reader not found. [name=%s][type=%s]",
                    inputContentInfo.reader(), inputContentInfo.contentType().name()));
        }
        return config.createInstance(inputContentInfo);
    }

    private InputReaderConfig findConfig(InputContentInfo inputContentInfo) {
        if (sourceTypeDefaults.containsKey(inputContentInfo.contentType())) {
            String r = sourceTypeDefaults.get(inputContentInfo.contentType());
            return readerConfigs.get(r);
        }
        for (String name : readerConfigs.keySet()) {
            InputReaderConfig config = readerConfigs.get(name);
            if (config.supports(inputContentInfo.contentType())) {
                return config;
            }
        }
        return null;
    }
}
