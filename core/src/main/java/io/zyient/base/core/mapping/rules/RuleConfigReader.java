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

package io.zyient.base.core.mapping.rules;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.ReflectionUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class RuleConfigReader<T> {
    public static final String __CONFIG_PATH = "rules";
    public static final String __CONFIG_PATH_RULE = "rule";

    private Class<? extends T> entityType;
    private RulesCache<T> cache;

    @SuppressWarnings("unchecked")
    public List<Rule<T>> read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            if (ConfigReader.checkIfNodeExists(xmlConfig, __CONFIG_PATH)) {
                List<HierarchicalConfiguration<ImmutableNode>> nodes = xmlConfig.configurationsAt(__CONFIG_PATH_RULE);
                List<Rule<T>> rules = new ArrayList<>(nodes.size());
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    ConfigReader reader = new ConfigReader(node, null, RuleConfig.class);
                    reader.read();
                    RuleConfig config = (RuleConfig) reader.settings();
                    if (config.isReference()) {
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
                        Field field = null;
                        if (config.getType() == RuleType.Transformation) {
                            field = ReflectionUtils.findField(entityType, config.getTarget());
                            if (field == null) {
                                throw new Exception(String.format("Target field not found. [type=%s][field=%s]",
                                        entityType.getCanonicalName(), config.getTarget()));
                            }
                        }
                        Rule<T> rule = null;
                        Class<? extends Rule<T>> type = (Class<? extends Rule<T>>) ConfigReader.readType(node);
                        if (type == null) {
                            rule = BaseRule.createDefaultInstance();
                        } else {
                            rule = type.getDeclaredConstructor()
                                    .newInstance();
                        }
                        rule.withEntityType(entityType)
                                .withTargetField(field)
                                .configure(config);
                        if (ConfigReader.checkIfNodeExists(node, __CONFIG_PATH)) {
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
