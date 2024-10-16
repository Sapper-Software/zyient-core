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

import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.EvaluationStatus;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.List;


public interface Rule<T> {
    String __ERROR_TYPE_RULES = "Rules";
    String __ERROR_TYPE_VALIDATION = "Validation";

    String name();

    Rule<T> withContentDir(@NonNull File contentDir);

    Rule<T> withEntityType(@NonNull Class<? extends T> type);

    Rule<T> withTerminateOnValidationError(boolean terminate);

    Rule<T> configure(@NonNull RuleConfig config,
                      @NonNull BaseEnv<?> env) throws ConfigurationException;

    EvaluationStatus evaluate(@NonNull T data) throws RuleValidationError, RuleEvaluationError;

    RuleType getRuleType();

    Class<? extends T> entityType();

    int errorCode();

    int validationErrorCode();

    void addSubRules(@NonNull List<Rule<T>> rules) throws Exception;

    Rule<T> addVisitor(@NonNull RuleVisitor<T> visitor);

    RuleVisitor<T> visitor();
}
