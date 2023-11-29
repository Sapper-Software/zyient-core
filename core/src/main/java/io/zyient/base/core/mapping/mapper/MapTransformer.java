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

package io.zyient.base.core.mapping.mapper;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.PropertyModel;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.mapping.model.MappedElement;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class MapTransformer<T> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class MapNode {
        private String name;
        private Class<?> type;
        private String targetPath;
        private Map<String, MapNode> nodes;
    }

    private final Class<? extends T> type;
    private final Map<String, MapNode> mapper = new HashMap<>();

    public MapTransformer(@NonNull Class<? extends T> type) {
        this.type = type;
    }

    public MapTransformer<T> add(@NonNull MappedElement element) throws Exception {
        PropertyModel pm = ReflectionHelper.findProperty(type, element.getTargetPath());
        if (pm == null) {
            throw new Exception(String.format("Target field not found. [class=%s][field=%s]",
                    type.getCanonicalName(), element.getTargetPath()));
        }
        MapNode node = findNode(element, pm);
        return this;
    }

    public Map<String, Object> transform(@NonNull Map<String, Object> source) throws Exception {
        Preconditions.checkState(!mapper.isEmpty());
        Map<String, Object> data = new HashMap<>();
        for (String key : mapper.keySet()) {
            MapNode node = mapper.get(key);
            transform(source, node, data);
        }
        return data;
    }

    @SuppressWarnings("unchecked")
    private void transform(Map<String, Object> source,
                           MapNode node,
                           Map<String, Object> data) throws Exception {
        if (source.containsKey(node.name)) {
            Object value = source.get(node.name);
            if (!Strings.isNullOrEmpty(node.targetPath)) {
                Map<String, Object> map = getTargetNode(data, node.targetPath);
                String[] parts = node.targetPath.split("\\.");
                String key = parts[parts.length - 1];
                map.put(key, value);
            } else if (value != null) {
                if (!(value instanceof Map<?, ?>)) {
                    throw new Exception(String.format("Invalid node type: Expected Map<String, Object> [type=%s]",
                            value.getClass().getCanonicalName()));
                }
                if (node.nodes != null && !node.nodes.isEmpty()) {
                    for (String key : node.nodes.keySet()) {
                        transform((Map<String, Object>) value, node.nodes.get(key), data);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getTargetNode(Map<String, Object> data, String targetPath) {
        String[] parts = targetPath.split("\\.");
        Map<String, Object> current = data;
        int index = 0;
        while (index < parts.length - 1) {
            String name = parts[index];
            if (current.containsKey(name)) {
                current = (Map<String, Object>) current.get(name);
            } else {
                Map<String, Object> nmap = new HashMap<>();
                current.put(name, nmap);
                current = nmap;
            }
            index++;
        }
        return current;
    }

    private MapNode findNode(MappedElement element,
                             PropertyModel property) throws Exception {
        String source = element.getSourcePath();
        String[] parts = source.split("\\.");
        if (parts.length == 1) {
            if (mapper.containsKey(parts[0])) {
                return mapper.get(parts[0]);
            } else {
                MapNode node = new MapNode();
                node.name = parts[0];
                node.targetPath = element.getTargetPath();
                node.type = property.field().getType();
                mapper.put(parts[0], node);
                return node;
            }
        } else {
            MapNode node = null;
            for (int ii = 0; ii < parts.length; ii++) {
                String name = parts[ii];
                if (node == null) {
                    if (mapper.containsKey(name)) {
                        node = mapper.get(name);
                    } else {
                        node = new MapNode();
                        node.name = name;
                        mapper.put(name, node);
                    }
                } else {
                    if (ii == parts.length - 1) {
                        if (node.nodes != null && node.nodes.containsKey(name)) {
                            node = node.nodes.get(name);
                            if (node.type != null && property.field() != null) {
                                if (node.type.equals(property.field().getType())) {
                                    throw new Exception(String.format("Type mis-match: [current=%s][specified=%s]",
                                            node.type.getCanonicalName(),
                                            property.field().getType().getCanonicalName()));
                                }
                            } else {
                                if (property.field() != null)
                                    node.type = property.field().getType();
                                node.targetPath = element.getTargetPath();
                            }
                        } else {
                            MapNode nnode = new MapNode();
                            if (property.field() != null)
                                nnode.type = property.field().getType();
                            nnode.name = name;
                            nnode.targetPath = element.getTargetPath();
                            if (node.nodes == null) {
                                node.nodes = new HashMap<>();
                            }
                            node.nodes.put(name, nnode);
                            node = nnode;
                        }
                    } else {
                        if (node.nodes != null && node.nodes.containsKey(name)) {
                            node = node.nodes.get(name);
                        } else {
                            MapNode nnode = new MapNode();
                            nnode.name = name;
                            if (node.nodes == null) {
                                node.nodes = new HashMap<>();
                            }
                            node.nodes.put(name, nnode);
                            node = nnode;
                        }
                    }
                }
            }
            return node;
        }
    }
}
