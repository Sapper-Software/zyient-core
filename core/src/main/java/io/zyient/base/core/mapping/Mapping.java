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
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.annotations.Ignore;
import io.zyient.base.core.mapping.annotations.Target;
import io.zyient.base.core.mapping.model.MappedElement;
import io.zyient.base.core.mapping.transformers.*;
import io.zyient.base.core.model.PropertyBag;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class Mapping<T> {
    public static final String __CONFIG_PATH = "mappings";
    private final Map<String, MappedElement> sourceIndex = new HashMap<>();
    private final Map<String, MappedElement> targetIndex = new HashMap<>();
    private final Class<? extends T> type;
    private final Map<String, Field> fieldTree = new HashMap<>();
    private final MappingProcessor processor;
    private MappingSettings settings;

    public Mapping(@NonNull Class<? extends T> type,
                   @NonNull MappingProcessor processor) {
        this.type = type;
        this.processor = processor;
    }

    public Mapping<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH);
            String cp = MappingSettings.class.getAnnotation(ConfigPath.class).path();
            if (ConfigReader.checkIfNodeExists(xmlConfig, cp)) {
                ConfigReader reader = new ConfigReader(xmlConfig, cp, MappingSettings.class);
                reader.read();
                settings = (MappingSettings) reader.settings();
            } else {
                settings = new MappingSettings();
            }
            readMappings(config);
            buildFieldTree(type, null);
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void buildFieldTree(Class<?> type, String prefix) throws Exception {
        Field[] fields = ReflectionUtils.getAllFields(type);
        Preconditions.checkNotNull(fields);
        for (Field field : fields) {
            if (field.isAnnotationPresent(Ignore.class)) continue;
            String name = field.getName();
            if (!Strings.isNullOrEmpty(prefix)) {
                name = String.format("%s.%s", prefix, name);
            }
            String target = null;
            if (field.isAnnotationPresent(Target.class)) {
                Target t = field.getAnnotation(Target.class);
                Preconditions.checkArgument(!Strings.isNullOrEmpty(t.name()));
                target = t.name();
                if (fieldTree.containsKey(target)) {
                    Field f = fieldTree.get(target);
                    throw new Exception(String.format("Duplicate target name used. [fields={%s, %s}, target=%s]",
                            f.getName(), field.getName(), target));
                }
            }

            if (ReflectionUtils.isPrimitiveTypeOrString(field) ||
                    field.getType().isEnum() ||
                    field.getType().equals(Date.class)) {
                fieldTree.put(name, field);
                if (!Strings.isNullOrEmpty(target)) {
                    fieldTree.put(target, field);
                }
            } else if (ReflectionUtils.isCollection(field)) {
                Class<?> inner = ReflectionUtils.getGenericCollectionType(field);
                if (!ReflectionUtils.isPrimitiveTypeOrString(inner)) {
                    throw new Exception(String.format("Collection type not supported. [type=%s]",
                            inner.getCanonicalName()));
                }
                fieldTree.put(name, field);
                if (!Strings.isNullOrEmpty(target)) {
                    fieldTree.put(target, field);
                }
            } else if (ReflectionUtils.isMap(field)) {
                Class<?> kt = ReflectionUtils.getGenericMapKeyType(field);
                if (!kt.equals(String.class)) {
                    throw new Exception(String.format("Map Key type not supported. [type=%s]", kt.getCanonicalName()));
                }
                Class<?> vt = ReflectionUtils.getGenericMapValueType(field);
                if (!ReflectionUtils.isPrimitiveTypeOrString(vt)) {
                    throw new Exception(String.format("Map Value type not supported. [type=%s]", kt.getCanonicalName()));
                }
                fieldTree.put(name, field);
                if (!Strings.isNullOrEmpty(target)) {
                    fieldTree.put(target, field);
                }
            } else {
                buildFieldTree(field.getType(), name);
            }
        }
    }

    private void readMappings(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        ConfigPath cp = MappedElement.class.getAnnotation(ConfigPath.class);
        Preconditions.checkNotNull(cp);
        Preconditions.checkState(!Strings.isNullOrEmpty(cp.path()));
        List<HierarchicalConfiguration<ImmutableNode>> maps = xmlConfig.configurationsAt(cp.path());
        if (maps != null && !maps.isEmpty()) {
            for (HierarchicalConfiguration<ImmutableNode> node : maps) {
                MappedElement me = MappedElement.read(node);
                sourceIndex.put(me.getSourcePath(), me);
                targetIndex.put(me.getTargetPath(), me);
            }
        } else {
            throw new Exception("No mappings found...");
        }
    }

    public T read(@NonNull Map<String, Object> source) throws Exception {
        T data = type.getDeclaredConstructor().newInstance();
        for (String path : sourceIndex.keySet()) {
            String[] parts = path.split("\\.");
            Object value = findSourceValue(source, parts, 0);
            MappedElement me = sourceIndex.get(path);
            if (value == null) {
                if (me.isMandatory()) {
                    throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                            me.getSourcePath(), me.getTargetPath()));
                }
                continue;
            }
            setFieldValue(me, value, data);
        }
        return data;
    }

    private void setFieldValue(MappedElement element, Object value, T data) throws Exception {
        if (element.isCustom()) {
            if (!(data instanceof PropertyBag)) {
                throw new Exception(String.format("Custom mapping not supported for type. [type=%s]",
                        data.getClass().getCanonicalName()));
            }
            Object tv = transform(value, element, element.getType());
            if (tv == null) {
                if (element.isMandatory()) {
                    throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                            element.getSourcePath(), element.getTargetPath()));
                }
            } else {
               ((PropertyBag) data).add(element.getTargetPath(), tv);
            }
        } else {
            Field field = fieldTree.get(element.getTargetPath());
            if (field == null) {
                throw new Exception(String.format("Target field not found. [path=%s]", element.getTargetPath()));
            }
            Object tv = transform(value, element, field.getType());
            if (tv == null) {
                if (element.isMandatory()) {
                    throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                            element.getSourcePath(), element.getTargetPath()));
                }
            } else {
                ReflectionUtils.setValue(tv, data, field);
            }
        }
    }

    private Object transform(Object value,
                             MappedElement element,
                             Class<?> type) throws Exception {
        if (type == null) {
            type = element.getType();
        }
        if (type == null) {
            type = String.class;
        }
        Transformer<?> transformer = null;
        if (element.getTransformer() != null) {
            transformer = element.getTransformer()
                    .getDeclaredConstructor()
                    .newInstance();
        } else {
            transformer = getDefaultTransformer(type, element);
        }
        if (transformer == null) {
            throw new Exception(String.format("Transformer not found for type. [type=%s]", type.getCanonicalName()));
        }
        return transformer.transform(value);
    }

    private Transformer<?> getDefaultTransformer(Class<?> type,
                                                 MappedElement element) {
        if (ReflectionUtils.isBoolean(type)) {
            return new BooleanTransformer();
        } else if (ReflectionUtils.isInt(type)) {
            return new IntegerTransformer()
                    .decimalSeparator(settings().getDecimalSeparator());
        } else if (ReflectionUtils.isFloat(type)) {
            return new FloatTransformer()
                    .decimalSeparator(settings().getDecimalSeparator());
        } else if (ReflectionUtils.isLong(type)) {
            return new LongTransformer()
                    .decimalSeparator(settings().getDecimalSeparator());
        } else if (ReflectionUtils.isDouble(type)) {
            return new DoubleTransformer()
                    .decimalSeparator(settings().getDecimalSeparator());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Object findSourceValue(Map<String, Object> node, String[] parts, int index) {
        String key = parts[index];
        if (node.containsKey(key)) {
            if (index == parts.length - 1) {
                return node.get(key);
            } else {
                Object o = node.get(key);
                if (o instanceof Map<?, ?>) {
                    findSourceValue((Map<String, Object>) o, parts, index + 1);
                }
            }
        }
        return null;
    }
}
