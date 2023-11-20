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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.model.MappedResponse;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class BaseRule<T> implements Rule<T> {
    @Config(name = "rule")
    private String rule;
    @Config(name = "target", required = false)
    private String target = null;
    @Config(name = "ruleType", required = false, type = RuleType.class)
    private RuleType ruleType = RuleType.Transformation;
    private Field targetField;
    private Class<? extends T> type;
    private List<Rule<T>> rules;


    @Override
    public Rule<T> withTargetField(Field targetField) throws Exception {
        if (ruleType == RuleType.Transformation) {
            if (targetField == null) {
                throw new Exception(String.format("Target field required for rule. [rule=%s]", rule));
            }
        }
        this.targetField = targetField;
        return this;
    }

    @Override
    public Rule<T> withType(@NonNull Class<? extends T> type) {
        this.type = type;
        return this;
    }


    @Override
    public Rule<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        if (getType() == null) {
            throw new ConfigurationException("Entity type not specified...");
        }
        if (getRuleType() == RuleType.Transformation) {
            if (getTargetField() == null) {
                throw new ConfigurationException(String.format("Target field not set. [target=%s]", getTarget()));
            }
        }
        try {
            setup(xmlConfig);
            if (ConfigReader.checkIfNodeExists(xmlConfig, Rule.__CONFIG_PATH)) {
                rules = new ArrayList<>();
                List<HierarchicalConfiguration<ImmutableNode>> nodes = xmlConfig.configurationsAt(Rule.__CONFIG_PATH_RULE);
                if (nodes == null || nodes.isEmpty()) {
                    throw new ConfigurationException("No rules defined in configuration...");
                }
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    Rule<T> rule = BaseRule.read(node, type);
                    rules.add(rule);
                }
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Object evaluate(@NonNull MappedResponse<T> data) throws Exception {
        try {
            Object response = doEvaluate(data);
            if (rules != null && !rules.isEmpty()) {
                for (Rule<T> rule : rules) {
                    response = rule.evaluate(data);
                }
            }
            return response;
        } catch (RuleConditionFailed cf) {
            return false;
        }
    }

    protected abstract Object doEvaluate(@NonNull MappedResponse<T> data) throws Exception;

    protected abstract void setup(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    @SuppressWarnings("unchecked")
    public static <T> Rule<T> read(@NonNull HierarchicalConfiguration<ImmutableNode> node,
                                   @NonNull Class<? extends T> entityType) throws Exception {
        Rule<T> rule = null;
        Class<? extends Rule<T>> type = (Class<? extends Rule<T>>) ConfigReader.readType(node);
        if (type == null) {
            rule = createDefaultInstance();
        } else {
            rule = type.getDeclaredConstructor().newInstance();
        }
        rule = ConfigReader.read(node, rule);
        Field field = null;
        if (rule.getRuleType() == RuleType.Transformation) {
            field = ReflectionUtils.findField(entityType, rule.getTarget());
            if (field == null) {
                throw new ConfigurationException(String.format("Field not found. [type=%s][field=%s]",
                        entityType.getCanonicalName(), rule.getTarget()));
            }
        }
        rule.withType(entityType)
                .withTargetField(field)
                .configure(node);
        return rule;
    }

    private static <T> Rule<T> createDefaultInstance() {
        return new SpELRule<T>();
    }
}
