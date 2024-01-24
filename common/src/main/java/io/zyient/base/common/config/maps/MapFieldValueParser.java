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

package io.zyient.base.common.config.maps;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.FieldValueParser;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class MapFieldValueParser<V> implements FieldValueParser<Map<String, V>> {
    public static final String __CONFIG_PATH = "map";
    public static final String __CONFIG_PATH_VALUES = "values";
    public static final String KEY_NAME = "name";
    public static final String KEY_VALUE = "value";

    private final String path;
    private final String valuesPath;
    private final String keyName;
    private final String valueName;

    public MapFieldValueParser() {
        path = __CONFIG_PATH;
        valuesPath = __CONFIG_PATH_VALUES;
        keyName = KEY_NAME;
        valueName = KEY_VALUE;
    }

    public MapFieldValueParser(String path,
                               @NonNull String valuesPath,
                               @NonNull String keyName,
                               @NonNull String valueName) {
        this.path = path;
        this.valuesPath = valuesPath;
        this.keyName = keyName;
        this.valueName = valueName;
    }


    @Override
    public Map<String, V> parse(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        try {
            if (Strings.isNullOrEmpty(path) || ConfigReader.checkIfNodeExists(config, path)) {
                Map<String, V> map = new HashMap<>();
                HierarchicalConfiguration<ImmutableNode> node = config.configurationAt(path);
                List<HierarchicalConfiguration<ImmutableNode>> nodes = node.configurationsAt(valuesPath);
                for (HierarchicalConfiguration<ImmutableNode> n : nodes) {
                    String key = n.getString(keyName).trim();
                    String value = n.getString(valueName).trim();
                    if (Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(value)) {
                        throw new Exception("Invalid Map configuration: Key and/or Value missing...");
                    }
                    map.put(key, fromString(value));
                }
                if (!map.isEmpty()) {
                    return map;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    protected abstract V fromString(@NonNull String value) throws Exception;
}
