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

import com.google.common.base.Preconditions;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.StatusCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class RuleGroup<T> implements Rule<T> {
    private RuleGroupConfig settings;
    private RulesEvaluator<T> evaluator;
    private List<Rule<T>> rules;
    private Class<? extends T> entityType;
    private int errorCode;
    private boolean terminateOnValidationError = true;

    @Override
    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    @Override
    public Rule<T> withContentDir(@NonNull File contentDir) {
        return this;
    }

    @Override
    public Rule<T> withEntityType(@NonNull Class<? extends T> type) {
        this.entityType = type;
        return this;
    }

    @Override
    public Rule<T> withTerminateOnValidationError(boolean terminate) {
        terminateOnValidationError = terminate;
        return this;
    }

    @Override
    public Rule<T> configure(@NonNull RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof RuleGroupConfig);
        settings = (RuleGroupConfig) config;
        if (config.getErrorCode() != null)
            errorCode = config.getErrorCode();
        return this;
    }

    @Override
    public EvaluationStatus evaluate(@NonNull T data) throws RuleValidationError, RuleEvaluationError {
        Preconditions.checkState(rules != null && !rules.isEmpty());
        synchronized (this) {
            if (evaluator == null) {
                evaluator = new RulesEvaluator<>(rules, terminateOnValidationError);
            }
        }
        EvaluationStatus status = new EvaluationStatus();
        try {
            status.setStatus(StatusCode.Success);
            evaluator.evaluate(data, status);
            if (status.getErrors() != null) {
                status.setStatus(StatusCode.ValidationFailed);
            }
        } catch (RuleValidationError ve) {
            if (terminateOnValidationError) {
                throw ve;
            }
            status.error(ve).setStatus(StatusCode.ValidationFailed);
        }
        return status;
    }

    @Override
    public RuleType getRuleType() {
        return RuleType.Group;
    }

    @Override
    public Class<? extends T> entityType() {
        return entityType;
    }

    @Override
    public int errorCode() {
        return errorCode;
    }

    @Override
    public int validationErrorCode() {
        return -1;
    }

    @Override
    public void addSubRules(@NonNull List<Rule<T>> rules) throws Exception {
        if (this.rules == null)
            this.rules = rules;
        else {
            this.rules.addAll(rules);
        }
    }
}
