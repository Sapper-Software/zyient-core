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

package io.zyient.core.filesystem;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.Settings;
import io.zyient.core.filesystem.model.Container;
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
public class ContainerConfigReader extends ConfigReader {
    public static final String __CONFIG_PATH = "containers";
    public static final String CONFIG_CONTAINER = "container";
    public static final String CONFIG_DEFAULT_CONTAINER = "default";

    private Map<String, Container> containers;
    private final ConfigReader reader;
    private Container defaultContainer;

    public ContainerConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                 @NonNull Class<? extends Container> containerType) {
        super(config, __CONFIG_PATH, containerType);
        reader = new ConfigReader(config, __CONFIG_PATH, containerType);
    }

    @Override
    public void read() throws ConfigurationException {
        try {
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config().configurationsAt(CONFIG_CONTAINER);
            if (nodes != null && !nodes.isEmpty()) {
                containers = new HashMap<>();
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    Settings settings = type().getDeclaredConstructor().newInstance();
                    Preconditions.checkState(settings instanceof Container);
                    settings = reader.read(settings, node);
                    Container container = (Container) settings;
                    containers.put(container.getDomain(), container);
                }
            } else {
                throw new ConfigurationException("No containers defined...");
            }
            String key = get().getString(CONFIG_DEFAULT_CONTAINER);
            checkStringValue(key, getClass(), CONFIG_DEFAULT_CONTAINER);
            defaultContainer = containers.get(key);
            if (defaultContainer == null) {
                throw new ConfigurationException(String.format("Invalid Default Container: [name=%s]", key));
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
