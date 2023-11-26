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

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.mapping.rules.spel.SpELRule;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class BaseRule<T> implements Rule<T> {
    private String name;
    private String rule;
    private RuleType ruleType = RuleType.Transformation;
    private Class<? extends T> entityType;
    private List<Rule<T>> rules;
    private int errorCode;
    private int validationErrorCode = -1;
    private RuleConfig config;

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
    public RuleType getRuleType() {
        return ruleType;
    }

    @Override
    public Rule<T> configure(@NonNull RuleConfig config) throws ConfigurationException {
        if (entityType == null) {
            throw new ConfigurationException("Entity type not specified...");
        }
        try {
            name = config.getName();
            rule = config.getRule();
            ruleType = config.getType();
            errorCode = config.getErrorCode();
            if (getRuleType() == RuleType.Validation) {
                if (config.getValidationErrorCode() == null) {
                    throw new Exception(String.format("Validation error code required for rule. [rule=%s]", name));
                }
                validationErrorCode = config.getValidationErrorCode();
            }
            setup(config);
            this.config = config;
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Object evaluate(@NonNull MappedResponse<T> data) throws Exception {
        ValidationExceptions errors = null;
        try {
            Object response = doEvaluate(data);
            if (rules != null && !rules.isEmpty()) {
                for (Rule<T> rule : rules) {
                    try {
                        response = rule.evaluate(data);
                        if (rule.getRuleType() == RuleType.Condition) {
                            Preconditions.checkNotNull(response);
                            if (!(response instanceof Boolean)) {
                                throw new Exception(String
                                        .format("Rule returned invalid response. [rule=%s][response=%s]",
                                                rule.name(), response.getClass().getCanonicalName()));
                            }
                            if (!((Boolean) response)) {
                                break;
                            }
                        }
                    } catch (RuleValidationError ve) {
                        errors = ValidationExceptions.add(ve, errors);
                    }
                }
            }
            if (errors != null) {
                throw errors;
            }
            return response;
        } catch (ValidationException ve) {
            errors = ValidationExceptions.add(ve, errors);
            throw errors;
        }
    }

    protected abstract Object doEvaluate(@NonNull MappedResponse<T> data) throws RuleValidationError,
            RuleEvaluationError;

    protected abstract void setup(@NonNull RuleConfig config) throws ConfigurationException;

    public static <T> Rule<T> createDefaultInstance() {
        return new SpELRule<T>();
    }
}
