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

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.mapping.model.MappedResponse;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.lang.reflect.Field;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class BaseRule<T> implements Rule<T> {
    private String name;
    private String rule;
    private String target = null;
    private RuleType ruleType = RuleType.Transformation;
    private Field targetField;
    private Class<? extends T> entityType;
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
    public Rule<T> withEntityType(@NonNull Class<? extends T> type) {
        this.entityType = type;
        return this;
    }

    @Override
    public void addSubRules(@NonNull List<Rule<T>> rules) {
        this.rules = rules;
    }

    @Override
    public String getTarget() {
        return target;
    }

    @Override
    public RuleType getRuleType() {
        return ruleType;
    }

    @Override
    public Rule<T> configure(@NonNull RuleConfig config) throws ConfigurationException {
        if (entityType == null) {
            throw new ConfigurationException("Entity type not specified...");
        }
        if (getRuleType() == RuleType.Transformation) {
            if (targetField == null) {
                throw new ConfigurationException(String.format("Target field not set. [target=%s]", getTarget()));
            }
        }
        try {
            name = config.getName();
            rule = config.getRule();
            target = config.getTarget();
            ruleType = config.getType();

            setup(config);
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

    protected abstract void setup(@NonNull RuleConfig config) throws ConfigurationException;

    public static <T> Rule<T> createDefaultInstance() {
        return new SpELRule<T>();
    }
}
