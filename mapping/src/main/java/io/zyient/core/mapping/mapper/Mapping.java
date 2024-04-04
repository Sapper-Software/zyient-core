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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.GlobalConstants;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.EDataTypes;
import io.zyient.base.common.model.entity.PropertyBag;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.common.utils.beans.PropertyDef;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.decisions.EvaluationTree;
import io.zyient.base.core.decisions.builder.EvaluationTreeBuilder;
import io.zyient.core.mapping.DataException;
import io.zyient.core.mapping.model.CurrencyValue;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.StatusCode;
import io.zyient.core.mapping.model.mapping.*;
import io.zyient.core.mapping.readers.MappingContextProvider;
import io.zyient.core.mapping.rules.*;
import io.zyient.core.mapping.transformers.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class Mapping<T> {
    public static final String __CONFIG_PATH = "mapping";
    public static final String __CONFIG_PATH_MAPPINGS = "mappings";
    public static final String __CONFIG_PATH_SERDE = "serdes";

    protected final Class<? extends T> entityType;
    protected final Class<? extends MappedResponse<T>> responseType;
    private File contentDir;
    protected final Map<Integer, Mapped> sourceIndex = new HashMap<>();
    private MappingSettings settings;
    private final Map<String, DeSerializer<?>> deSerializers = new HashMap<>();
    private RulesExecutor<MappedResponse<T>> rulesExecutor;
    private MappingContextProvider contextProvider;
    private RulesCache<MappedResponse<T>> rulesCache;
    private IMapTransformer<T> mapTransformer;
    private FilterChain<SourceMap> filterChain;
    private ObjectMapper mapper;
    private boolean terminateOnValidationError = false;
    private StringTransformer stringTransformer;
    private EvaluationTree<Map<String, Object>, ConditionalMappedElement> evaluationTree;
    protected BaseEnv<?> env;

    protected Mapping(@NonNull Class<? extends T> entityType, @NonNull Class<? extends MappedResponse<T>> responseType) {
        this.entityType = entityType;
        this.responseType = responseType;
    }

    public Mapping<T> withContentDir(@NonNull File contentDir) {
        this.contentDir = contentDir;
        return this;
    }

    @SuppressWarnings("unchecked")
    public Mapping<T> withRulesCache(RulesCache<?> rulesCache) {
        this.rulesCache = (RulesCache<MappedResponse<T>>) rulesCache;
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

    public Mapping<T> withTransformer(IMapTransformer<T> transformer) {
        this.mapTransformer = transformer;
        return this;
    }

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    public Mapping<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig, @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkNotNull(entityType);
        this.env = env;
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig;
            if (config.getRootElementName().compareTo(__CONFIG_PATH) != 0) {
                config = xmlConfig.configurationAt(__CONFIG_PATH);
            }
            mapper = GlobalConstants.getJsonMapper();
            String cp = MappingSettings.class.getAnnotation(ConfigPath.class).path();
            ConfigReader reader = new ConfigReader(xmlConfig, cp, MappingSettings.class);
            reader.read();
            settings = (MappingSettings) reader.settings();
            settings.postLoad();
            stringTransformer = new StringTransformer(this).useJson(settings().isUseJsonForString());
            readDeSerializers(config);
            readMappings(config);
            if (!deSerializers.isEmpty()) {
                SimpleModule module = new SimpleModule();
                for (String name : deSerializers.keySet()) {
                    DeSerializer<?> deSerializer = deSerializers.get(name);
                    addDeSerializer(module, name);
                }
                mapper.registerModule(module);
            }
            checkAndLoadFilters(config);
            checkAndLoadRules(config);
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void checkAndLoadFilters(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        if (ConfigReader.checkIfNodeExists(xmlConfig, FilterChain.__CONFIG_PATH)) {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(FilterChain.__CONFIG_PATH);
            filterChain = new FilterChain<>(SourceMap.class).withContentDir(contentDir).configure(config, env);
        }
    }

    @SuppressWarnings("unchecked")
    private void readDeSerializers(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        if (ConfigReader.checkIfNodeExists(xmlConfig, __CONFIG_PATH_SERDE)) {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH_SERDE);
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(DeSerializer.__CONFIG_PATH);
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                Class<? extends DeSerializer<?>> type = (Class<? extends DeSerializer<?>>) ConfigReader.readType(node);
                if (type == null) {
                    throw new Exception("Class not specified for DeSerializer...");
                }
                DeSerializer<?> deSerializer = type.getDeclaredConstructor().newInstance().configure(node);
                deSerializers.put(deSerializer.name(), deSerializer);
            }
        }
        DeSerializer<?> deSerializer = null;
        deSerializer = new IntegerTransformer().locale(settings.getLocale()).configure(settings);
        deSerializers.put(deSerializer.name(), deSerializer);
        deSerializer = new FloatTransformer().locale(settings.getLocale()).configure(settings);
        deSerializers.put(deSerializer.name(), deSerializer);
        deSerializer = new LongTransformer().locale(settings.getLocale()).configure(settings);
        deSerializers.put(deSerializer.name(), deSerializer);
        deSerializer = new DoubleTransformer().locale(settings.getLocale()).configure(settings);
        deSerializers.put(deSerializer.name(), deSerializer);
        deSerializer = new DateTransformer().locale(settings.getLocale()).format(settings.getDateFormat()).configure(settings);
        deSerializers.put(deSerializer.name(), deSerializer);
    }

    @SuppressWarnings("unchecked")
    private <S> void addDeSerializer(SimpleModule module, String name) {
        DeSerializer<S> deSerializer = (DeSerializer<S>) deSerializers.get(name);
        module.addDeserializer(deSerializer.type(), deSerializer);
    }

    private void checkAndLoadRules(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        if (ConfigReader.checkIfNodeExists(xmlConfig, RuleConfigReader.__CONFIG_PATH)) {
            rulesExecutor = new RulesExecutor<MappedResponse<T>>(responseType)
                    .terminateOnValidationError(terminateOnValidationError)
                    .cache(rulesCache)
                    .contentDir(contentDir)
                    .configure(xmlConfig, env);
        }
    }

    @SuppressWarnings("unchecked")
    private void readMappings(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        HierarchicalConfiguration<ImmutableNode> mNode = xmlConfig.configurationAt(__CONFIG_PATH_MAPPINGS);
        ConfigPath cp = Mapped.class.getAnnotation(ConfigPath.class);
        Preconditions.checkNotNull(cp);
        Preconditions.checkState(!Strings.isNullOrEmpty(cp.path()));
        mapTransformer = buildTransformer(mNode, cp.path());
        if (ConfigReader.checkIfNodeExists(mNode, EvaluationTreeBuilder.__CONFIG_PATH)) {
            HierarchicalConfiguration<ImmutableNode> bNode = mNode.configurationAt(EvaluationTreeBuilder.__CONFIG_PATH);
            Class<? extends EvaluationTreeBuilder<Map<String, Object>, ConditionalMappedElement>> type
                    = (Class<? extends EvaluationTreeBuilder<Map<String, Object>, ConditionalMappedElement>>) ConfigReader.readType(bNode);
            if (type == null) {
                throw new Exception("Evaluation Tree builder type not specified...");
            }
            EvaluationTreeBuilder<Map<String, Object>, ConditionalMappedElement> builder
                    = type.getDeclaredConstructor()
                    .newInstance()
                    .configure(bNode, env);
            evaluationTree = builder.build();
        }
    }

    @SuppressWarnings("unchecked")
    public IMapTransformer<T> buildTransformer(HierarchicalConfiguration<ImmutableNode> mNode,
                                               String configPath) throws Exception {
        IMapTransformer<T> mapTransformer = new MapTransformer<>(entityType, settings);
        List<HierarchicalConfiguration<ImmutableNode>> maps = mNode.configurationsAt(configPath);
        if (maps != null && !maps.isEmpty()) {
            for (HierarchicalConfiguration<ImmutableNode> node : maps) {
                Class<? extends MappedElement> type = (Class<? extends MappedElement>) ConfigReader.readType(node);
                if (type == null) {
                    type = MappedElement.class;
                }
                Mapped m = Mapped.read(node, type, env);
                sourceIndex.put(m.getSequence(), m);
                if (m instanceof MappedElement me) {
                    if (me.getMappingType() == MappingType.Field || me.getMappingType() == MappingType.ConstField) {
                        mapTransformer.add(me);
                    }
                }
            }
        }
        return mapTransformer;
    }

    public MappedResponse<T> read(@NonNull SourceMap source, Context context) throws Exception {
        MappedResponse<T> response = new MappedResponse<T>(source);
        response.setContext(context);
        if (filterChain != null) {
            StatusCode s = filterChain.evaluate(source);
            if (s == StatusCode.IgnoreRecord) {
                if (DefaultLogger.isTraceEnabled()) {
                    DefaultLogger.trace("IGNORED RECORD", source);
                }
                EvaluationStatus status = new EvaluationStatus();
                status.setStatus(s);
                response.setStatus(status);
                return response;
            }
        }
        Map<String, Object> converted = mapTransformer.transform(source, entityType, context);
        T entity = mapper.convertValue(converted, entityType);
        response.setEntity(entity);

        for (Integer index : sourceIndex.keySet()) {

            Mapped m = sourceIndex.get(index);
            if (m instanceof MappedElement me) {
                if (me.getMappingType() == MappingType.Field || me.getMappingType() == MappingType.ConstField) continue;
                executeMapping(me, response, source, context);
            }
        }
        if (evaluationTree != null) {
            ConditionalMappedElement element = evaluationTree.evaluate(source);
            if (element != null) {
                for (MappedElement me : element.getMappings()) {
                    executeMapping(me, response, source, context);
                }
            }
        }
        EvaluationStatus status;
        if (rulesExecutor != null) {
            status = rulesExecutor.evaluate(response);
        } else {
            status = new EvaluationStatus();
            status.setStatus(StatusCode.Success);
        }
        response.setStatus(status);
        return response;
    }

    private void executeMapping(MappedElement me,
                                MappedResponse<T> response,
                                SourceMap source,
                                Context context) throws Exception {
        Object value = null;
        String path = me.getSourcePath();
        if (me instanceof WildcardMappedElement wme && ReflectionHelper.implementsInterface(PropertyBag.class, entityType)) {
            T data = response.getEntity();
            int valCount = 1;
            for (String key : source.keySet()) {
                ((PropertyBag) data).setProperty(String.format("%s%d", wme.getPrefix(), valCount), source.get(key));
                valCount++;
            }
        } else if (MappingReflectionHelper.isContextPrefixed(path)) {
            value = findContextValue(context, path);
        } else if (me.getMappingType() == MappingType.ConstProperty || me.getMappingType() == MappingType.ConstField) {
            value = me.getSourcePath();
        } else {
            String[] parts = path.split("\\.");
            value = findSourceValue(source, parts, 0);
        }
        if (value == null) {
            if (!me.isNullable()) {
                throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                        me.getSourcePath(), me.getTargetPath()));
            }
            return;
        }
        setFieldValue(me, value, response);
        if (me.getVisitor() != null) {
            me.getVisitor().visit(me, response, source, context);
        }
    }

    private void setFieldValue(MappedElement element, Object value, MappedResponse<T> response) throws Exception {
        T data = response.getEntity();
        if (element.getMappingType() == MappingType.Property) {
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
        } else if (element.getMappingType() == MappingType.Cached || element.getMappingType() == MappingType.ConstProperty) {
            Object tv = transform(value, element, element.getType());
            if (tv == null) {
                if (!element.isNullable()) {
                    throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                            element.getSourcePath(), element.getTargetPath()));
                }
            } else {
                response.add(element.getTargetPath(), tv);
            }
        } else if (element.getMappingType() == MappingType.Field || element.getMappingType() == MappingType.ConstField) {
            Object tv = transform(value, element, element.getType());
            if (tv == null) {
                if (!element.isNullable()) {
                    throw new DataException(String.format("Required field value is missing. [source=%s][field=%s]",
                            element.getSourcePath(), element.getTargetPath()));
                }
            } else {
                PropertyDef def = ReflectionHelper.findProperty(entityType, element.getTargetPath());
                if (def == null) {
                    throw new Exception(String.format("Property not found. [entity=%s][property=%s]",
                            entityType.getCanonicalName(), element.getTargetPath()));
                }
                MappingReflectionHelper.setProperty(element.getTargetPath(), def, data, tv);
            }
        }
    }

    private Object transform(Object value, MappedElement element, Class<?> type) throws Exception {
        if (type == null) {
            type = element.getType();
        }
        if (type == null) {
            type = String.class;
        }
        if (element.getDataType() != null) {
            DeSerializer<?> deSerializer = null;
            if (element.getDataType() == EDataTypes.Currency) {
                deSerializer = getDeSerializer(CurrencyValue.class, element);
            } else if (element.getDataType() == EDataTypes.Json) {
                if (element.getTargetType() == null) {
                    throw new Exception(String.format("Target type required. [element=%s]", element.getTargetPath()));
                }
                if (ReflectionHelper.isSuperType(element.getTargetType(), value.getClass())) {
                    return value;
                } else if (value instanceof String) {
                    return JSONUtils.read((String) value, element.getTargetType());
                } else {
                    throw new Exception(String.format("Failed to de-serialize from type. [type=%s]",
                            value.getClass().getCanonicalName()));
                }
            } else {
                deSerializer = getDeSerializer(EDataTypes.asJavaType(element.getDataType()),
                        element);
            }
            if (deSerializer == null) {
                throw new Exception(String.format("Failed to get de-serializer. [type=%s]",
                        element.getDataType().name()));
            }
            return deSerializer.transform(value);
        }
        if (element.getClass().equals(MappedElement.class)) {
            if (type.equals(String.class)) {
                return stringTransformer.serialize(value);
            } else if (ReflectionHelper.isSuperType(type, value.getClass())) {
                return value;
            } else if (type.isInterface() && ReflectionHelper.implementsInterface(type, value.getClass())) {
                return value;
            }
        }

        return mapTransformer.transform(element, value);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public DeSerializer<?> getDeSerializer(@NonNull Class<?> type, MappedElement element) throws Exception {
        if (deSerializers.containsKey(type.getCanonicalName())) {
            return deSerializers.get(type.getCanonicalName());
        }
        DeSerializer<?> deSerializer = null;
        if (type.isEnum()) {
            if (deSerializers.containsKey(type.getSimpleName())) {
                return deSerializers.get(type.getSimpleName());
            }
            if (element instanceof EnumMappedElement) {
                deSerializer = new EnumTransformer(type).enumValues(((EnumMappedElement) element)
                        .getEnumMappings())
                        .configure(settings);
            } else {
                deSerializer = new EnumTransformer(type).configure(settings);
            }
        } else if (ReflectionHelper.isSuperType(CurrencyValue.class, type)) {
            deSerializer = new CurrencyValueTransformer().locale(settings.getLocale()).configure(settings);
        } else if (ReflectionHelper.isSuperType(Date.class, type)) {
            if (deSerializers.containsKey(Date.class.getCanonicalName())) {
                return deSerializers.get(Date.class.getCanonicalName());
            }
            deSerializer = new DateTransformer()
                    .locale(settings.getLocale())
                    .format(settings.getDateFormat())
                    .configure(settings);
        }
        if (deSerializer != null) {
            deSerializers.put(deSerializer.name(), deSerializer);
        }
        return deSerializer;
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
                    return findSourceValue((Map<String, Object>) o, parts, index + 1);
                }
            }
        }
        return null;
    }

    private Object findContextValue(Context context, String field) {
        return MappingReflectionHelper.getContextProperty(field, context);
    }
}
