package io.zyient.core.mapping.mapper;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.common.utils.beans.PropertyDef;
import io.zyient.core.mapping.model.mapping.CustomMappedElement;
import io.zyient.core.mapping.model.mapping.MappedElement;
import io.zyient.core.mapping.model.mapping.MappingType;
import io.zyient.core.mapping.model.mapping.RegexMappedElement;
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
public class MapTransformer<T> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class MapNode {
        private String name;
        private Class<?> type;
        private String targetPath;
        private Map<String, MapNode> nodes;
        private Transformer<?> transformer;
        private boolean nullable;
        private MappingType mappingType;
    }

    private final Class<? extends T> type;
    private final MappingSettings settings;
    private final Map<String, MapNode> mapper = new HashMap<>();
    private final Map<String, Transformer<?>> transformers = new HashMap<>();

    public MapTransformer(@NonNull Class<? extends T> type, @NonNull MappingSettings settings) {
        this.type = type;
        this.settings = settings;
    }

    public MapTransformer<T> add(@NonNull MappedElement element) throws Exception {
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
                        .configure(settings);
                transformers.put(transformer.name(), transformer);
                return transformer;
            }
        } else if (elem instanceof RegexMappedElement element) {
            if (transformers.containsKey(element.getName())) {
                return transformers.get(element.getName());
            } else if (create) {
                RegexTransformer transformer = (RegexTransformer) new RegexTransformer()
                        .withGroups(element.getGroups())
                        .regex(element.getRegex())
                        .format(element.getFormat())
                        .replace(element.getReplace())
                        .configure(settings);
                transformers.put(element.getName(), transformer);
                return transformer;
            }
        }
        return null;
    }

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

    public Map<String, Object> transform(@NonNull Map<String, Object> source,
                                         @NonNull Class<? extends T> entityType) throws Exception {
        Preconditions.checkState(!mapper.isEmpty());
        Map<String, Object> data = new HashMap<>();
        JSONUtils.checkAndAddType(data, entityType);
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
        Object value = null;
        if (node.mappingType == MappingType.ConstField || node.mappingType == MappingType.ConstProperty) {
            value = node.name;
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
                        transform((Map<String, Object>) value, node.nodes.get(key), data);
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

    private MapNode findNode(MappedElement element,
                             PropertyDef property) throws Exception {
        String source = element.getSourcePath();
        String[] parts = null;
        if (element.getMappingType() == MappingType.ConstField
                || element.getMappingType() == MappingType.ConstProperty) {
            parts = new String[]{source};
        } else {
            parts = source.split("\\.");
        }
        if (parts.length == 1) {
            if (mapper.containsKey(parts[0])) {
                return mapper.get(parts[0]);
            } else {
                MapNode node = new MapNode();
                node.name = parts[0];
                node.targetPath = element.getTargetPath();
                node.type = property.field().getType();
                node.nullable = element.isNullable();
                node.mappingType = element.getMappingType();
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
                            nnode.nullable = element.isNullable();
                            nnode.mappingType = element.getMappingType();
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