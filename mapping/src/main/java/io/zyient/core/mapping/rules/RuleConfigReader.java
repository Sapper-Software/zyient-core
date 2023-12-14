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

package io.zyient.core.mapping.rules;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.errors.Errors;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class RuleConfigReader<T> {
    public static final String __CONFIG_PATH = "rules";
    public static final String __CONFIG_PATH_RULE = "rule";
    public static final String __CONFIG_ATTR_TYPE = "settings";

    private Class<? extends T> entityType;
    private RulesCache<T> cache;
    private File contentDir;

    @SuppressWarnings("unchecked")
    public List<Rule<T>> read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            if (Errors.getDefault() == null) {
                throw new ConfigurationException("Errors cache not initialized...");
            }
            if (ConfigReader.checkIfNodeExists(xmlConfig, __CONFIG_PATH)) {
                HierarchicalConfiguration<ImmutableNode> root = xmlConfig.configurationAt(__CONFIG_PATH);
                List<HierarchicalConfiguration<ImmutableNode>> nodes = root.configurationsAt(__CONFIG_PATH_RULE);
                List<Rule<T>> rules = new ArrayList<>(nodes.size());
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    Class<? extends RuleConfig> rType = (Class<? extends RuleConfig>) ConfigReader
                            .readType(node, __CONFIG_ATTR_TYPE);
                    if (rType == null) {
                        throw new ConfigurationException("Rule configuration type missing...");
                    }
                    ConfigReader reader = new ConfigReader(node, null, rType);
                    reader.read();
                    RuleConfig config = (RuleConfig) reader.settings();
                    config.validate();
                    if (config instanceof RuleReferenceConfig) {
                        if (cache == null) {
                            throw new Exception(String
                                    .format("Reference rule specified, but cache is not available. [rule=%s]",
                                            config.getName()));
                        }
                        Rule<T> rule = cache.get(config.getName());
                        if (rule == null) {
                            throw new Exception(String.format("Referenced rule not found in cache. [name=%s]",
                                    config.getName()));
                        }
                        rules.add(rule);
                    } else {
                        Rule<T> rule = config.createInstance(entityType);
                        rule.withEntityType(entityType)
                                .withContentDir(contentDir)
                                .configure(config);
                        if (ConfigReader.checkIfNodeExists(node, __CONFIG_PATH)) {
                            if (rule.getRuleType() != RuleType.Group
                                    && rule.getRuleType() != RuleType.Condition) {
                                throw new Exception(String
                                        .format("[rule=%s] Sub-rules can only be added to Rule group or condition Rule. [type=%s]",
                                                rule.name(), rule.getRuleType().name()));
                            }
                            List<Rule<T>> subRules = read(node);
                            if (subRules != null) {
                                rule.addSubRules(subRules);
                            }
                        }
                        rules.add(rule);
                    }
                }
                return rules;
            }
            return null;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
