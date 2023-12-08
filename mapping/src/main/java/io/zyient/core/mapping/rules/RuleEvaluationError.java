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

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class RuleEvaluationError extends Exception {
    private final String rule;
    private final String entityType;
    private final String field;
    private final Throwable inner;
    private final int errorCode;
    private final String message;

    public RuleEvaluationError(@NonNull String rule,
                               @NonNull Class<?> entityType,
                               @NonNull String field,
                               @NonNull Integer errorCode,
                               @NonNull String message,
                               @NonNull Throwable inner) {
        super(String.format("[CODE=%d][rule=%s][entity=%s][field=%s] %s",
                errorCode,
                rule,
                entityType.getCanonicalName(),
                field,
                message));
        this.errorCode = errorCode;
        this.entityType = entityType.getCanonicalName();
        this.field = field;
        this.rule = rule;
        this.message = message;
        this.inner = inner;
    }

    public RuleEvaluationError(@NonNull String rule,
                               @NonNull Class<?> entityType,
                               @NonNull String field,
                               @NonNull Integer errorCode,
                               @NonNull String message) {
        super(String.format("[CODE=%d][rule=%s][entity=%s][field=%s] %s",
                errorCode,
                rule,
                entityType.getCanonicalName(),
                field,
                message));
        this.errorCode = errorCode;
        this.entityType = entityType.getCanonicalName();
        this.field = field;
        this.rule = rule;
        this.message = message;
        this.inner = null;
    }
}
