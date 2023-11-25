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

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.readers.settings.ReaderFactorySettings;
import io.zyient.base.core.mapping.rules.RulesCache;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapperFactory {
    public static final String __CONFIG_PATH = "mappers";
    public static final String __CONFIG_PATH_FACTORY = String.format("%s.factory", __CONFIG_PATH);
    public static final String __KEY_CLASS = "class";
    public static final String __KEY_CONFIG_PATH = "config";

    private MapperFactorySettings settings;
    private final Map<String, Mapping<?>> mappings = new HashMap<>();
    private final Map<Class<?>, RulesCache<?>> rulesCaches = new HashMap<>();


    public MapperFactory init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH);
            HierarchicalConfiguration<ImmutableNode> factoryConfig = xmlConfig.configurationAt(__CONFIG_PATH_FACTORY);
            ConfigReader reader = new ConfigReader(factoryConfig, null, ReaderFactorySettings.class);
            reader.read();
            settings = (MapperFactorySettings) reader.settings();
            readGlobalRules(xmlConfig);
            readMappingConfigs(config);
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    private void readGlobalRules(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        if (!ConfigReader.checkIfNodeExists(xmlConfig, RulesCacheSettings.__CONFIG_PATH)) {
            return;
        }
        HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(RulesCacheSettings.__CONFIG_PATH);
        List<HierarchicalConfiguration<ImmutableNode>> nodes =
                config.configurationsAt(RulesCacheSettings.__CONFIG_PATH_CACHE);
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            ConfigReader reader = new ConfigReader(node, null, RulesCacheSettings.class);
            reader.read();
            RulesCacheSettings cacheSettings = (RulesCacheSettings) reader.settings();
            File cf = new File(PathUtils.formatPath(String.format("%s/%s",
                    settings.getConfigDir(), cacheSettings.getFilename())));
            if (!cf.exists()) {
                throw new ConfigurationException(String.format("Mapping configuration file not found. [path=%s]",
                        cf.getAbsolutePath()));
            }
            XMLConfiguration rConfig = ConfigReader.readFromFile(cf.getAbsolutePath());
            HierarchicalConfiguration<ImmutableNode> rNode =
                    rConfig.configurationAt(RulesCacheSettings.__CONFIG_PATH_GLOBAL);
            RulesCache<?> cache = createCache(cacheSettings.getEntityType())
                    .configure(rNode, cacheSettings.getEntityType());
            rulesCaches.put(cacheSettings.getEntityType(), cache);
        }
    }

    private <E> RulesCache<E> createCache(Class<? extends E> type) {
        return new RulesCache<E>();
    }

    @SuppressWarnings("unchecked")
    private void readMappingConfigs(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        for (String name : settings.getMappingConfigs().keySet()) {
            String v = settings.getMappingConfigs().get(name);
            Map<String, String> map = ReflectionUtils.mapFromString(v);
            if (map == null) {
                throw new ConfigurationException(String.format("Invalid mapping configuration: [value=%s]", v));
            }
            String cls = map.get(__KEY_CLASS);
            if (Strings.isNullOrEmpty(cls)) {
                throw new ConfigurationException(String
                        .format("Invalid mapping configuration: missing mapping class. [value=%s]", v));
            }
            Class<? extends Mapping<?>> mCls = (Class<? extends Mapping<?>>) Class.forName(cls);
            String cfg = map.get(__KEY_CONFIG_PATH);
            if (Strings.isNullOrEmpty(cls)) {
                throw new ConfigurationException(String
                        .format("Invalid mapping configuration: missing mapping configuration path. [value=%s]", v));
            }
            File cf = new File(PathUtils.formatPath(String.format("%s/%s", settings.getConfigDir(), cfg)));
            if (!cf.exists()) {
                throw new ConfigurationException(String.format("Mapping configuration file not found. [path=%s]",
                        cf.getAbsolutePath()));
            }
            XMLConfiguration xmlConfig = ConfigReader.readFromFile(cf.getAbsolutePath());
            HierarchicalConfiguration<ImmutableNode> mConfig = xmlConfig.configurationAt(Mapping.__CONFIG_PATH);
            Class<?> entityType = ConfigReader.readType(mConfig, "entity");
            RulesCache<?> cache = rulesCaches.get(entityType);
            Mapping<?> mapping = createInstance(entityType, mCls)
                    .withRulesCache(cache)
                    .configure(mConfig);
            mappings.put(mapping.name(), mapping);
        }
    }

    @SuppressWarnings("unchecked")
    private <E> Mapping<E> createInstance(Class<? extends E> entityType,
                                          Class<? extends Mapping<?>> cls) throws Exception {
        return (Mapping<E>) cls.getDeclaredConstructor().newInstance();
    }

    public Mapping<?> findMapping(@NonNull InputContentInfo inputContentInfo) throws Exception {
        if (Strings.isNullOrEmpty(inputContentInfo.mapping())) {
            throw new Exception(String.format("Mapper name not specified. [content id=%s]", inputContentInfo.documentId()));
        }
        if (mappings.containsKey(inputContentInfo.mapping())) {
            return mappings.get(inputContentInfo.mapping());
        }
        throw new Exception(String.format("Mapper not found. [name=%s][content id=%s]",
                inputContentInfo.mapping(), inputContentInfo.documentId()));
    }

    @SuppressWarnings("unchecked")
    public <T> Mapping<T> getMapping(@NonNull String name) {
        return (Mapping<T>) mappings.get(name);
    }
}