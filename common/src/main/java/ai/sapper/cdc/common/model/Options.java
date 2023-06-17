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

package ai.sapper.cdc.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class Options {
    public static final String __CONFIG_PATH = "options";

    private String configPath;

    private Map<String, Object> options = new HashMap<>();

    public Options() {
        configPath = __CONFIG_PATH;
    }

    public Options(@NonNull String configPath) {
        this.configPath = configPath;
    }

    public Options(@NonNull Options source) {
        this.configPath = source.configPath;
        if (!source.options.isEmpty())
            options.putAll(source.options);
    }

    public Options(@NonNull Map<String, Object> options) {
        configPath = __CONFIG_PATH;
        if (!options.isEmpty())
            this.options.putAll(options);
    }

    public boolean containsKey(@NonNull String name) {
        return options.containsKey(name);
    }

    public Object get(@NonNull String name) {
        if (options.containsKey(name)) {
            return options.get(name);
        }
        return null;
    }

    public Object put(@NonNull String name, Object value) {
        return options.put(name, value);
    }

    public Object remove(@NonNull String name) {
        return options.remove(name);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return options.isEmpty();
    }

    public int size() {
        return options.size();
    }

    public void clear() {
        options.clear();
    }

    public Optional<Boolean> getBoolean(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof Boolean) {
                return Optional.of((Boolean) o);
            }
        }
        return Optional.empty();
    }

    public Optional<Integer> getInt(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof Integer) {
                return Optional.of((Integer) o);
            }
        }
        return Optional.empty();
    }

    public Optional<Long> getLong(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof Long) {
                return Optional.of((Long) o);
            }
        }
        return Optional.empty();
    }

    public Optional<Double> getDouble(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof Double) {
                return Optional.of((Double) o);
            }
        }
        return Optional.empty();
    }

    public Optional<String> getString(@NonNull String key) {
        if (options.containsKey(key)) {
            Object o = options.get(key);
            if (o instanceof String) {
                return Optional.of((String) o);
            }
        }
        return Optional.empty();
    }


    public void read(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        HierarchicalConfiguration<ImmutableNode> node = config.configurationAt(configPath);
        List<HierarchicalConfiguration<ImmutableNode>> nodes = node.configurationsAt(Option.Constants.__CONFIG_PATH);
        if (nodes != null && !nodes.isEmpty()) {
            for (HierarchicalConfiguration<ImmutableNode> n : nodes) {
                Option option = new Option().read(n);
                put(option.getName(), option.parseValue());
            }
        }
    }
}
