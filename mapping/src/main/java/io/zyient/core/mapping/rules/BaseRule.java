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

package io.zyient.core.mapping.rules;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.StatusCode;
import io.zyient.core.mapping.rules.spel.SpELRule;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class BaseRule<T> implements Rule<T> {
    private String name;
    private String expression;
    private RuleType ruleType = RuleType.Transformation;
    private Class<? extends T> entityType;
    private List<Rule<T>> rules;
    private int errorCode;
    private int validationErrorCode = -1;
    private RuleConfig config;
    private boolean terminateOnValidationError = true;
    private RulesEvaluator<T> evaluator;
    private BaseEnv<?> env;
    private RuleVisitor<T> visitor;

    @Override
    public Rule<T> withEntityType(@NonNull Class<? extends T> type) {
        this.entityType = type;
        return this;
    }

    @Override
    public void addSubRules(@NonNull List<Rule<T>> rules) {
        if (this.rules == null)
            this.rules = rules;
        else {
            this.rules.addAll(rules);
        }
    }

    @Override
    public Rule<T> withTerminateOnValidationError(boolean terminate) {
        terminateOnValidationError = terminate;
        return this;
    }

    @Override
    public RuleType getRuleType() {
        return ruleType;
    }

    @Override
    public Rule<T> configure(@NonNull RuleConfig cfg,
                             @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkArgument(cfg instanceof BaseRuleConfig);
        BaseRuleConfig config = (BaseRuleConfig) cfg;
        if (entityType == null) {
            throw new ConfigurationException("Entity type not specified...");
        }
        this.env = env;
        try {
            name = config.getName();
            expression = config.getExpression();
            ruleType = config.getType();
            errorCode = config.getErrorCode();
            if (config.getValidationErrorCode() != null)
                validationErrorCode = config.getValidationErrorCode();
            setup(config);
            this.config = config;
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public EvaluationStatus evaluate(@NonNull T data) throws RuleValidationError, RuleEvaluationError {
        if (rules != null) {
            synchronized (this) {
                if (evaluator == null) {
                    evaluator = new RulesEvaluator<>(rules, terminateOnValidationError);
                }
            }
        }
        EvaluationStatus status = new EvaluationStatus();
        try {
            Object response = doEvaluate(data);
            status.setResponse(response);
            RuleType rt = getRuleType();
            if (rt == RuleType.Filter || rt == RuleType.Condition) {
                if (response != null) {
                    if (response instanceof Boolean) {
                        boolean r = (boolean) response;
                        if (rt == RuleType.Condition) {
                            if (!r) {
                                status.setStatus(StatusCode.Failed);
                                return status;
                            }
                        } else {
                            if (r) {
                                status.setStatus(StatusCode.IgnoreRecord);
                                return status;
                            }
                        }
                    }
                }
                if (this.terminateOnValidationError) {
                    throw new RuleEvaluationError(name,
                            entityType,
                            getRuleType().name(),
                            errorCode,
                            "Invalid Filter/Condition Rule response.");
                }

            }
            status.setStatus(StatusCode.Success);
            if (visitor != null) {
                visitor.onSuccess(data, status);
            }
            if (evaluator != null) {
                evaluator.evaluate(data, status);
            }
            if (status.getErrors() != null) {
                status.setStatus(StatusCode.ValidationFailed);
            }
            return status;
        } catch (RuleValidationError ve) {
            if (visitor != null) {
                visitor.onError(ve, data);
            }
            if (terminateOnValidationError) {
                throw ve;
            }
            status.error(ve).setStatus(StatusCode.ValidationFailed);
            return status;
        } catch (Throwable t) {
            if (visitor != null) {
                visitor.onError(t, data);
            }
            throw t;
        }
    }

    @Override
    public Rule<T> addVisitor(@NonNull RuleVisitor<T> visitor) {
        this.visitor = visitor;
        return this;
    }

    protected abstract Object doEvaluate(@NonNull T data) throws RuleValidationError,
            RuleEvaluationError;

    public abstract void setup(@NonNull RuleConfig config) throws ConfigurationException;

    public static <T> Rule<T> createDefaultInstance() {
        return new SpELRule<T>();
    }
}
