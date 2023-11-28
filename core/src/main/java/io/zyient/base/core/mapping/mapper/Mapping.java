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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.mapping.DataException;
import io.zyient.base.core.mapping.model.*;
import io.zyient.base.core.mapping.readers.MappingContextProvider;
import io.zyient.base.core.mapping.rules.RuleConfigReader;
import io.zyient.base.core.mapping.rules.RulesCache;
import io.zyient.base.core.mapping.rules.RulesExecutor;
import io.zyient.base.core.mapping.transformers.*;
import io.zyient.base.core.model.PropertyBag;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class Mapping<T> {
    public static final String __CONFIG_PATH = "mapping";

    private Class<? extends T> entityType;
    private File contentDir;
    private final Map<String, MappedElement> sourceIndex = new HashMap<>();
    private final Map<String, MappedElement> targetIndex = new HashMap<>();
    private MappingSettings settings;
    private final Map<String, DeSerializer<?>> transformers = new HashMap<>();
    private RulesExecutor<T> rulesExecutor;
    private MappingContextProvider contextProvider;
    private RulesCache<T> rulesCache;
    private MapTransformer<T> mapTransformer;
    private final ObjectMapper mapper = new ObjectMapper();
    private boolean terminateOnValidationError = false;

    public Mapping<T> withEntityType(@NonNull Class<? extends T> entityType) {
        this.entityType = entityType;
        return this;
    }

    public Mapping<T> withContentDir(@NonNull File contentDir) {
        this.contentDir = contentDir;
        return this;
    }

    @SuppressWarnings("unchecked")
    public Mapping<T> withRulesCache(RulesCache<?> rulesCache) {
        this.rulesCache = (RulesCache<T>) rulesCache;
        return this;
    }

    public Mapping<T> withContextProvider(MappingContextProvider contextProvider) {
        this.contextProvider = contextProvider;
        return this;
    }

    public Mapping<T> withTerminateOnValidationError(boolean terminateOnValidationError) {
        this.terminateOnValidationError = terminateOnValidationError;
        return this;
    }

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    @SuppressWarnings("unchecked")
    public Mapping<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        Preconditions.checkNotNull(entityType);
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig;
            if (config.getRootElementName().compareTo(__CONFIG_PATH) != 0) {
                config = xmlConfig.configurationAt(__CONFIG_PATH);
            }
            String cp = MappingSettings.class.getAnnotation(ConfigPath.class).path();
            ConfigReader reader = new ConfigReader(xmlConfig, cp, MappingSettings.class);
            reader.read();
            settings = (MappingSettings) reader.settings();
            settings.postLoad();
            readMappings(config);
            if (!transformers.isEmpty()) {
                SimpleModule module = new SimpleModule();
                for (String name : transformers.keySet()) {
                    DeSerializer<?> deSerializer = transformers.get(name);
                    addDeSerializer(module, name);
                }
                mapper.registerModule(module);
            }
            checkAndLoadRules(config);
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <S> void addDeSerializer(SimpleModule module, String name) {
        DeSerializer<S> deSerializer = (DeSerializer<S>) transformers.get(name);
        module.addDeserializer(deSerializer.type(), deSerializer);
    }

    private void checkAndLoadRules(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        if (ConfigReader.checkIfNodeExists(xmlConfig, RuleConfigReader.__CONFIG_PATH)) {
            rulesExecutor = new RulesExecutor<T>(entityType)
                    .cache(rulesCache)
                    .contentDir(contentDir)
                    .configure(xmlConfig);
        }
    }

    @SuppressWarnings("unchecked")
    private void readMappings(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        ConfigPath cp = MappedElement.class.getAnnotation(ConfigPath.class);
        Preconditions.checkNotNull(cp);
        Preconditions.checkState(!Strings.isNullOrEmpty(cp.path()));
        List<HierarchicalConfiguration<ImmutableNode>> maps = xmlConfig.configurationsAt(cp.path());
        if (maps != null && !maps.isEmpty()) {
            mapTransformer = new MapTransformer<>(entityType);
            for (HierarchicalConfiguration<ImmutableNode> node : maps) {
                Class<? extends MappedElement> type = (Class<? extends MappedElement>) ConfigReader.readType(node);
                if (type == null) {
                    type = MappedElement.class;
                }
                MappedElement me = MappedElement.read(node, type);
                sourceIndex.put(me.getSourcePath(), me);
                targetIndex.put(me.getTargetPath(), me);
                if (me.getMappingType() == MappingType.Field) {
                    mapTransformer.add(me);
                    Field field = ReflectionHelper.findField(this.entityType, me.getTargetPath());
                    Preconditions.checkNotNull(field);
                    DeSerializer<?> transformer = getTransformer(field.getType(), me);
                    if (transformer != null) {
                        DefaultLogger.info(String.format("Using transformer. [type=%s][name=%s]",
                                field.getType(), transformer.name()));
                    }
                }
            }
        } else {
            throw new Exception("No mappings found...");
        }
    }

    public MappedResponse<T> read(@NonNull Map<String, Object> source, Context context) throws Exception {
        Map<String, Object> converted = mapTransformer.transform(source);
        T entity = mapper.convertValue(converted, entityType);
        MappedResponse<T> response = new MappedResponse<T>(source)
                .context(context);
        response.entity(entity);

        for (String path : sourceIndex.keySet()) {
            MappedElement me = sourceIndex.get(path);
            if (me.getMappingType() == MappingType.Field) continue;
            String[] parts = path.split("\\.");
            Object value = findSourceValue(source, parts, 0);
            if (value == null) {
                if (!me.isNullable()) {
                    throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                            me.getSourcePath(), me.getTargetPath()));
                }
                continue;
            }
            setFieldValue(me, value, response);
        }
        if (rulesExecutor != null) {
            rulesExecutor.evaluate(response, terminateOnValidationError);
        }
        return response;
    }

    private void setFieldValue(MappedElement element,
                               Object value,
                               MappedResponse<T> response) throws Exception {
        T data = response.entity();
        if (element.getMappingType() == MappingType.Custom) {
            if (!(data instanceof PropertyBag)) {
                throw new Exception(String.format("Custom mapping not supported for type. [type=%s]",
                        data.getClass().getCanonicalName()));
            }
            Object tv = transform(value, element, element.getType());
            if (tv == null) {
                if (!element.isNullable()) {
                    throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                            element.getSourcePath(), element.getTargetPath()));
                }
            } else {
                ((PropertyBag) data).setProperty(element.getTargetPath(), tv);
            }
        } else if (element.getMappingType() == MappingType.Cached) {
            Object tv = transform(value, element, element.getType());
            if (tv == null) {
                if (!element.isNullable()) {
                    throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                            element.getSourcePath(), element.getTargetPath()));
                }
            } else {
                response.add(element.getTargetPath(), tv);
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
        DeSerializer<?> transformer = null;
        if (element instanceof CustomMappedElement) {
            transformer = getTransformer(null, element);
        } else {
            transformer = getTransformer(type, element);
        }
        if (transformer == null) {
            throw new Exception(String.format("Transformer not found for type. [type=%s]", type.getCanonicalName()));
        }
        return transformer.transform(value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private DeSerializer<?> getTransformer(Class<?> type,
                                           MappedElement element) throws Exception {
        if (type != null && transformers.containsKey(type.getCanonicalName())) {
            return transformers.get(type.getCanonicalName());
        } else if (element instanceof CustomMappedElement) {
            if (!Strings.isNullOrEmpty(((CustomMappedElement) element).getTransformerName())) {
                if (transformers.containsKey(((CustomMappedElement) element).getTransformerName())) {
                    return transformers.get(((CustomMappedElement) element).getTransformerName());
                }
            }
        }
        DeSerializer<?> transformer = null;
        if (type == null && element instanceof CustomMappedElement) {
            transformer = ((CustomMappedElement) element).getTransformer()
                    .getDeclaredConstructor()
                    .newInstance()
                    .configure(settings);
        } else if (ReflectionHelper.isInt(type)) {
            transformer = new IntegerTransformer()
                    .locale(settings.getLocale())
                    .configure(settings);
        } else if (ReflectionHelper.isFloat(type)) {
            transformer = new FloatTransformer()
                    .locale(settings.getLocale())
                    .configure(settings);
        } else if (ReflectionHelper.isLong(type)) {
            transformer = new LongTransformer()
                    .locale(settings.getLocale())
                    .configure(settings);
        } else if (ReflectionHelper.isDouble(type)) {
            transformer = new DoubleTransformer()
                    .locale(settings.getLocale())
                    .configure(settings);
        } else if (type.equals(Date.class)) {
            transformer = new DateTransformer()
                    .locale(settings.getLocale())
                    .format(settings.getDateFormat())
                    .configure(settings);
        } else if (type.isEnum()) {
            if (transformers.containsKey(type.getSimpleName())) {
                return transformers.get(type.getSimpleName());
            }
            if (element instanceof EnumMappedElement) {
                transformer = new EnumTransformer(type)
                        .enumValues(((EnumMappedElement) element).getEnumMappings())
                        .configure(settings);
            } else {
                transformer = new EnumTransformer(type)
                        .configure(settings);
            }
        } else if (ReflectionHelper.isSuperType(CurrencyValue.class, type)) {
            transformer = new CurrencyValueTransformer()
                    .locale(settings.getLocale())
                    .configure(settings);
        } else if (element instanceof RegexMappedElement re) {
            if (transformers.containsKey(re.getName())) {
                return transformers.get(re.getName());
            }
            transformer = new RegexTransformer()
                    .regex(re.getRegex())
                    .name(re.getName());
            ((RegexTransformer) transformer).format(re.getFormat())
                    .groups(re.getGroups())
                    .replace(re.getReplace())
                    .configure(settings);
        }
        if (transformer != null) {
            transformers.put(transformer.name(), transformer);
        }
        return transformer;
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
