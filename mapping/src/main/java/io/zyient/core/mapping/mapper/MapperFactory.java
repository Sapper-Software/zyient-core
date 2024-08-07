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

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.rules.RulesCache;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class MapperFactory {
    public static final String __CONFIG_PATH = "mappers";
    public static final String __CONFIG_PATH_FACTORY = String.format("%s.factory", __CONFIG_PATH);

    private MapperFactorySettings settings;
    private final Map<String, Mapping<?>> mappings = new HashMap<>();
    private final Map<Class<?>, RulesCache<?>> rulesCaches = new HashMap<>();
    private File contentDir;
    private BaseEnv<?> env;
    private File mappingFile;

    public MapperFactory init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
        try {
            HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH);
            HierarchicalConfiguration<ImmutableNode> factoryConfig = xmlConfig.configurationAt(__CONFIG_PATH_FACTORY);
            ConfigReader reader = new ConfigReader(factoryConfig, null, MapperFactorySettings.class);
            reader.read();
            settings = (MapperFactorySettings) reader.settings();
            settings.postLoad(reader.config());
            contentDir = new File(settings.getContentDir());
            if (!contentDir.exists()) {
                throw new IOException(String.format("Content directory not found. [path=%s]",
                        contentDir.getAbsolutePath()));
            }
            readGlobalRules(xmlConfig);
            if (mappingFile != null) {
                readMappingConfigsFromFile(mappingFile);
            } else {
                readMappingConfigs(config);
            }

            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    public MapperFactory withMappingFile(File file) {
        this.mappingFile = file;
        return this;
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
                    settings.getContentDir(), cacheSettings.getFilename())));
            if (!cf.exists()) {
                throw new ConfigurationException(String.format("Mapping configuration file not found. [path=%s]",
                        cf.getAbsolutePath()));
            }
            XMLConfiguration rConfig = ConfigReader.readFromFile(cf.getAbsolutePath());
            HierarchicalConfiguration<ImmutableNode> rNode =
                    rConfig.configurationAt(RulesCacheSettings.__CONFIG_PATH_GLOBAL);
            RulesCache<?> cache = createCache(cacheSettings.getEntityType())
                    .contentDir(contentDir)
                    .configure(rNode, env, cacheSettings.getEntityType());
            rulesCaches.put(cacheSettings.getEntityType(), cache);
        }
    }

    private <E> RulesCache<E> createCache(Class<? extends E> type) {
        return new RulesCache<E>();
    }

    public void readMappingConfigsFromFile(File cf) throws Exception {
        XMLConfiguration xmlConfig = ConfigReader.readFromFile(cf.getAbsolutePath());
        HierarchicalConfiguration<ImmutableNode> mConfig = xmlConfig.configurationAt(Mapping.__CONFIG_PATH);
        Class<?> entityType = ConfigReader.readType(mConfig, "entity");
        RulesCache<?> cache = rulesCaches.get(entityType);
        Mapping<?> mapping = createInstance(mConfig)
                .withRulesCache(cache)
                .withContentDir(contentDir)
                .configure(mConfig, env,this);
        mappings.put(mapping.name(), mapping);
    }

    private void readMappingConfigs(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        for (String name : settings.getMappingConfigs().keySet()) {
            String cfg = settings.getMappingConfigs().get(name);
            if (Strings.isNullOrEmpty(cfg)) {
                throw new ConfigurationException("Invalid mapping configuration: missing mapping configuration path.");
            }
            File cf = new File(PathUtils.formatPath(String.format("%s/%s", settings.getContentDir(), cfg)));
            if (!cf.exists()) {
                throw new ConfigurationException(String.format("Mapping configuration file not found. [path=%s]",
                        cf.getAbsolutePath()));
            }
            readMappingConfigsFromFile(cf);
        }
    }

    public void readMappingConfig(@NonNull String config) throws Exception {
        File source = new File(config);
        if (!source.exists()) {
            throw new IOException(String.format("Mapping file not found. [path=%s]", source.getAbsolutePath()));
        }
        XMLConfiguration xmlConfig = ConfigReader.readFromFile(source.getAbsolutePath());
        HierarchicalConfiguration<ImmutableNode> mConfig = xmlConfig.configurationAt(Mapping.__CONFIG_PATH);
        Class<?> entityType = ConfigReader.readType(mConfig, "entity");
        RulesCache<?> cache = rulesCaches.get(entityType);
        Mapping<?> mapping = createInstance(mConfig)
                .withRulesCache(cache)
                .withContentDir(contentDir)
                .configure(mConfig, env, this);
        mappings.put(mapping.name(), mapping);
    }

    @SuppressWarnings("unchecked")
    private <E> Mapping<E> createInstance(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        Class<? extends Mapping<E>> type = (Class<? extends Mapping<E>>) ConfigReader.readType(config);
        if (type == null) {
            throw new Exception("Mapper type not specified...");
        }
        return type.getDeclaredConstructor()
                .newInstance();
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
