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

package io.zyient.core.mapping.model;

import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigReader;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class Column {
    public static final String __CONFIG_PATH = "columns";
    public static final String __CONFIG_PATH_COLUMN = "column";

    @Config(name = "name")
    private String name;
    @Config(name = "index", type = Integer.class)
    private Integer index;

    public Column() {
    }

    public Column(String name, Integer index) {
        this.name = name;
        this.index = index;
    }

    public static Map<Integer, Column> read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                            @NonNull Class<? extends Column> type) throws Exception {
        return read(xmlConfig, __CONFIG_PATH, type);
    }

    public static Map<Integer, Column> read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                            @NonNull String path,
                                            @NonNull Class<? extends Column> type) throws Exception {
        if (ConfigReader.checkIfNodeExists(xmlConfig, path)) {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(path);
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(__CONFIG_PATH_COLUMN);
            Map<Integer, Column> columns = new HashMap<>(nodes.size());
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                Column column = ConfigReader.read(node, type);
                columns.put(column.index, column);
            }
            return columns;
        }
        return null;
    }
}
