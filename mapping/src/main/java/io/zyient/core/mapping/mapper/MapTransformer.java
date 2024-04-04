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

package io.zyient.core.mapping.mapper;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.common.utils.beans.PropertyDef;
import io.zyient.core.mapping.model.mapping.CustomMappedElement;
import io.zyient.core.mapping.model.mapping.MappedElement;
import io.zyient.core.mapping.model.mapping.MappingType;
import io.zyient.core.mapping.model.mapping.RegexMappedElement;
import io.zyient.core.mapping.rules.MappingReflectionHelper;
import io.zyient.core.mapping.transformers.RegexTransformer;
import io.zyient.core.mapping.transformers.Transformer;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class MapTransformer<T> implements IMapTransformer<T>{


    private final Class<? extends T> type;
    private final MappingSettings settings;
    private final Map<String, Map<Integer, MapNode>> mapper = new HashMap<>();
    private final Map<String, Transformer<?>> transformers = new HashMap<>();


    public MapTransformer(@NonNull Class<? extends T> type, @NonNull MappingSettings settings) {
        this.type = type;
        this.settings = settings;
    }

    @Override
    public IMapTransformer<T> add(@NonNull MappedElement element) throws Exception {
        PropertyDef pm = ReflectionHelper.findProperty(type, element.getTargetPath());
        if (pm == null) {
            throw new Exception(String.format("Target field not found. [class=%s][field=%s]",
                    type.getCanonicalName(), element.getTargetPath()));
        }
        MapNode node = findNode(element, pm);
        if (element instanceof CustomMappedElement
                || element instanceof RegexMappedElement) {
            node.transformer = findTransformer(element, true);
        }
        return this;
    }

    private Transformer<?> findTransformer(MappedElement elem, boolean create) throws Exception {
        if (elem instanceof CustomMappedElement element) {
            if (transformers.containsKey(element.getTransformer())) {
                Transformer<?> transformer = transformers.get(element.getTransformer());
                if (!transformer.getClass().equals(element.getTransformerClass())) {
                    throw new Exception(String.format("Transformer type mismatch. [current=%s][requested=%s]",
                            transformer.getClass().getCanonicalName(), element.getTransformerClass().getCanonicalName()));
                }
                return transformer;
            } else if (create) {
                Transformer<?> transformer = element.getTransformerClass().getDeclaredConstructor()
                        .newInstance()
                        .configure(settings, element);
                transformers.put(transformer.name(), transformer);
                return transformer;
            }
        } else if (elem instanceof RegexMappedElement element) {
            if (transformers.containsKey(element.getName())) {
                return transformers.get(element.getName());
            } else if (create) {
                RegexTransformer transformer = (RegexTransformer) new RegexTransformer()
                        .configure(settings, element);
                transformers.put(transformer.name(), transformer);
                return transformer;
            }
        }
        return null;
    }
    @Override
    public Object transform(@NonNull MappedElement element, @NonNull Object source) throws Exception {
        if (element instanceof CustomMappedElement) {
            Transformer<?> transformer = findTransformer(element, true);
            if (transformer == null) {
                throw new Exception(String.format("Transformer not found. [name=%s]",
                        ((CustomMappedElement) element).getTransformer()));
            }
            return transformer.read(source);
        } else if (element instanceof RegexMappedElement re) {
            Transformer<?> transformer = findTransformer(re, true);
            if (transformer == null) {
                throw new Exception(String.format("Transformer not found. [name=%s]",
                        re.getName()));
            }
            return transformer.read(source);
        }
        return source;
    }


    @Override
    public Map<String, Object> transform(@NonNull Map<String, Object> source,
                                         @NonNull Class<? extends T> entityType, @NonNull Context context) throws Exception {
        Preconditions.checkState(!mapper.isEmpty());
        Map<String, Object> data = new HashMap<>();
        JSONUtils.checkAndAddType(data, entityType);
        for (String key : mapper.keySet()) {
            Map<Integer, MapNode> nodes = mapper.get(key);
            for (Integer seq : nodes.keySet()) {
                transform(source, nodes.get(seq), data, context.params);
            }
        }
        return data;
    }

    private void transform(Map<String, Object> source,
                           MapNode node,
                           Map<String, Object> data, Map<String, Object> contextParam) throws Exception {
        if (source.containsKey(node.name)
                || node.mappingType == MappingType.ConstField
                || node.mappingType == MappingType.ConstProperty) {
            findValueFromSourceOrContext(source, node, data);
        } else if (MappingReflectionHelper.isContextPrefixed(node.name)) {
            findValueFromSourceOrContext(contextParam, node, data);
        } else {
            if (!node.nullable) {
                throw new Exception(String.format("Field is not nullable. [field=%s]", node.targetPath));
            }
        }
    }

    private void findValueFromSourceOrContext(Map<String, Object> source,
                                              MapNode node,
                                              Map<String, Object> data) throws Exception {
        Object value = null;
        if (node.mappingType == MappingType.ConstField
                || node.mappingType == MappingType.ConstProperty) {
            value = node.name;
        } else if (MappingReflectionHelper.isContextPrefixed(node.name)) {
            Context dummyContext = new Context();
            dummyContext.setParams(source);
            value = MappingReflectionHelper.getContextProperty(node.name, dummyContext);
        } else if (source.containsKey(node.name)) {
            value = source.get(node.name);
            if (value == null) {
                if (!node.nullable) {
                    throw new Exception(String.format("Field is not nullable. [field=%s]", node.targetPath));
                }
                return;
            }
        }
        if (value != null) {
            if (!Strings.isNullOrEmpty(node.targetPath)) {
                Map<String, Object> map = getTargetNode(data, node.targetPath);
                String[] parts = node.targetPath.split("\\.");
                String key = parts[parts.length - 1];
                if (key.contains("[")) {
                    key = ReflectionHelper.extractKey(key);
                }
                if (node.transformer != null) {
                    value = node.transformer.read(value);
                }
                map.put(key, value);
            } else {
                if (!(value instanceof Map<?, ?>)) {
                    throw new Exception(String.format("Invalid node type: Expected Map<String, Object> [type=%s]",
                            value.getClass().getCanonicalName()));
                }
                if (node.nodes != null && !node.nodes.isEmpty()) {
                    for (String key : node.nodes.keySet()) {
                        transform((Map<String, Object>) value, node.nodes.get(key), data, source);
                    }
                }
            }
        } else {
            if (!node.nullable) {
                throw new Exception(String.format("Field is not nullable. [field=%s]", node.targetPath));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getTargetNode(Map<String, Object> data,
                                              String targetPath) throws Exception {
        String[] parts = targetPath.split("\\.");
        Map<String, Object> current = data;
        int index = 0;
        Class<?> currentType = type;
        while (index < parts.length - 1) {
            String name = parts[index];
            PropertyDef f = ReflectionHelper.findProperty(currentType, name);
            if (f == null) {
                throw new Exception(String.format("Field not found. [type=%s][name=%s]",
                        currentType.getCanonicalName(), name));
            }
            currentType = f.type();
            if (f.type() == null) {
                throw new Exception(String.format("Property type is null. [property=%s]", f.name()));
            }
            if (current.containsKey(name)) {
                current = (Map<String, Object>) current.get(name);
            } else {
                Map<String, Object> nmap = new HashMap<>();
                JSONUtils.checkAndAddType(nmap, currentType);
                current.put(name, nmap);
                current = nmap;
            }
            index++;
        }
        return current;
    }

    private MapNode getOrCreate(MappedElement element, String sourceKey, Class<?> cls) {
        Map<Integer, MapNode> seqMap = null;
        if (mapper.containsKey(sourceKey)) {
            seqMap = mapper.get(sourceKey);
            MapNode existingNode = seqMap.get(element.getSequence());
            if (existingNode != null) {
                return seqMap.get(element.getSequence());
            }
        }
        if (seqMap == null) {
            seqMap = new HashMap<>();
        }
        MapNode node = new MapNode();
        node.name = sourceKey;
        node.targetPath = element.getTargetPath();
        node.type = cls;
        node.nullable = element.isNullable();
        node.mappingType = element.getMappingType();
        seqMap.put(element.getSequence(), node);
        mapper.put(sourceKey, seqMap);
        return node;

    }

    private MapNode findNode(MappedElement element, PropertyDef property) throws Exception {
        if (element.getMappingType() == MappingType.ConstField
                || element.getMappingType() == MappingType.ConstProperty) {
            return getOrCreate(element, element.getSourcePath(), property.field().getType());
        }
        String source = element.getSourcePath();
        String[] parts = source.split("\\.");
        if (parts.length == 1) {
            String sourceKey = parts[0];
            return getOrCreate(element, sourceKey, property.field().getType());
        } else {
            MapNode node = null;
            for (int ii = 0; ii < parts.length; ii++) {
                String name = parts[ii];
                if (node == null) {
                    if (mapper.containsKey(name) && mapper.get(name).get(element.getSequence()) != null) {
                        node = mapper.get(name).get(element.getSequence());
                    } else {
                        node = new MapNode();
                        node.name = name;
                        if (mapper.containsKey(name)) {
                            mapper.get(name).put(element.getSequence(), node);
                        } else {
                            Map<Integer, MapNode> seqMap = new HashMap<>();
                            seqMap.put(element.getSequence(), node);
                            mapper.put(name, seqMap);
                        }
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
                                if (property.field() != null) node.type = property.field().getType();
                                node.targetPath = element.getTargetPath();
                            }
                        } else {
                            MapNode nnode = new MapNode();
                            if (property.field() != null) nnode.type = property.field().getType();
                            nnode.name = name;
                            nnode.targetPath = element.getTargetPath();
                            nnode.nullable = element.isNullable();
                            if (node.nodes == null) {
                                node.nodes = new HashMap<>();
                            }
                            node.mappingType = element.getMappingType();
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
