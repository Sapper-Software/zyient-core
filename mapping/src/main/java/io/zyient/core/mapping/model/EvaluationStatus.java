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

package io.zyient.core.mapping.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.core.mapping.rules.RuleValidationError;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class EvaluationStatus {
    private StatusCode status;
    private ValidationExceptions errors;
    private Object response;

    public EvaluationStatus error(@NonNull RuleValidationError error) {
        errors = ValidationExceptions.add(error, errors);
        return this;
    }

    public EvaluationStatus error(@NonNull String rule,
                                  @NonNull Class<?> entityType,
                                  @NonNull String field,
                                  int errorCode,
                                  @NonNull String message,
                                  @NonNull Throwable error) {
        errors = ValidationExceptions.add(new RuleValidationError(rule, entityType, field, errorCode, message, error), errors);
        return this;
    }

    public EvaluationStatus error(@NonNull String rule,
                                  @NonNull Class<?> entityType,
                                  @NonNull String field,
                                  int errorCode,
                                  @NonNull String message) {
        errors = ValidationExceptions.add(new RuleValidationError(rule, entityType, field, errorCode, message), errors);
        return this;
    }
}
