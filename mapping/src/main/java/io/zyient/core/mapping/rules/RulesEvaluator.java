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
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.StatusCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.List;

@Getter
@Accessors(fluent = true)
public class RulesEvaluator<T> {
    private final List<Rule<T>> rules;
    private final boolean terminateOnValidationError;

    public RulesEvaluator(@NonNull List<Rule<T>> rules,
                          boolean terminateOnValidationError) {
        Preconditions.checkArgument(!rules.isEmpty());
        this.rules = rules;
        this.terminateOnValidationError = terminateOnValidationError;
    }

    public void evaluate(@NonNull T data, EvaluationStatus status) throws RuleEvaluationError, RuleValidationError {
        for (Rule<T> rule : rules) {
            try {
                EvaluationStatus r = rule.evaluate(data);
                if (r.getErrors() != null) {
                    ValidationExceptions errors = r.getErrors();
                    for (ValidationException ve : errors.getErrors()) {
                        if (!(ve instanceof RuleValidationError)) {
                            throw new RuleEvaluationError(rule.name(),
                                    rule.entityType(),
                                    rule.getRuleType().name(),
                                    rule.errorCode(),
                                    String.format("Invalid validation error: [type=%s]",
                                            ve.getClass().getCanonicalName()));
                        }
                        if (terminateOnValidationError) {
                            throw (RuleValidationError) ve;
                        }
                        status.error((RuleValidationError) ve);
                    }
                }

                if (rule.getRuleType() == RuleType.Condition) {
                    if (r.getStatus() == StatusCode.Failed) {
                        status.setStatus(StatusCode.Failed);
                        break;
                    }
                } else if (rule.getRuleType() == RuleType.Filter) {
                    if (r.getStatus() == StatusCode.IgnoreRecord) {
                        status.setStatus(StatusCode.IgnoreRecord);
                        break;
                    }
                }
            } catch (RuleValidationError ve) {
                if (terminateOnValidationError) {
                    throw ve;
                }
                status.error(ve);
            }
        }
    }
}
