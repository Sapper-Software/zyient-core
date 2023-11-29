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

package io.zyient.base.core.mapping;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.mapping.model.ContentInfo;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.model.OutputContentInfo;
import io.zyient.base.core.mapping.readers.FileTypeDetector;
import io.zyient.base.core.mapping.readers.MappingContextProvider;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DemoContextProvider implements MappingContextProvider {
    private static final String REGEX_CUSTOMER_FILE = "(.*customers_.+)\\.(.+)";
    private static final String KEY_CUSTOMER_MAPPING = "customers";

    @Getter
    @Setter
    public static class DemoContextSettings extends Settings {
        @Config(name = "readers", type = Map.class)
        private Map<String, String> readers;
        @Config(name = "mappings", type = Map.class)
        private Map<String, String> mappings;
    }

    private DemoContextSettings settings;
    private final Map<SourceTypes, String> types = new HashMap<>();

    @Override
    public MappingContextProvider configure(HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        ConfigReader reader = new ConfigReader(config, null, DemoContextSettings.class);
        reader.read();
        settings = (DemoContextSettings) reader.settings();
        for (String t : settings.readers.keySet()) {
            SourceTypes st = SourceTypes.valueOf(t);
            types.put(st, settings.readers.get(t));
        }
        return this;
    }

    @Override
    public InputContentInfo inputContext(@NonNull ContentInfo contentInfo) throws Exception {
        Preconditions.checkArgument(contentInfo instanceof InputContentInfo);
        InputContentInfo info = (InputContentInfo) contentInfo;
        File file = info.path();
        if (file == null) {
            throw new Exception("Input file not specified...");
        }
        if (!file.exists()) {
            throw new IOException(String.format("Input file not found. [path=%s]", file.getAbsolutePath()));
        }
        SourceTypes st = info.contentType();
        if (st == null) {
            FileTypeDetector detector = new FileTypeDetector(file);
            detector.detect();
            st = detector.type();
            if (st == null) {
                throw new Exception(String.format("Failed to detect content type. [file=%s]", file.getAbsolutePath()));
            }
            info.contentType(st);
        }
        if (!types.containsKey(st)) {
            throw new Exception(String.format("Content type not supported. [type=%s]", st.name()));
        }
        URI uri = info.sourceURI();
        if (uri == null) {
            throw new Exception("Source URI not specified...");
        }
        if (Strings.isNullOrEmpty(info.mapping())) {
            Pattern p = Pattern.compile(REGEX_CUSTOMER_FILE);
            Matcher m = p.matcher(uri.toString());
            if (m.matches()) {
                String mapping = settings.mappings.get(KEY_CUSTOMER_MAPPING);
                if (Strings.isNullOrEmpty(mapping)) {
                    throw new Exception(String.format("Customer mapping not found. [key=%s]", KEY_CUSTOMER_MAPPING));
                }
                info.mapping(mapping);
            }
        }
        if (Strings.isNullOrEmpty(info.mapping())) {
            throw new Exception(String.format("Failed to get mapping for URI. [uri=%s]", uri.toString()));
        }
        return info;
    }

    @Override
    public OutputContentInfo outputContext(@NonNull ContentInfo contentInfo) throws Exception {
        return null;
    }
}
