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

import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.StatusCode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class RulesExecutor<T> {
    private final Class<? extends T> type;
    @Setter(AccessLevel.NONE)
    private List<Rule<T>> rules;
    private RulesCache<T> cache;
    private File contentDir;
    private RulesEvaluator<T> evaluator;
    private boolean terminateOnValidationError;
    private BaseEnv<?> env;

    public RulesExecutor(@NonNull Class<? extends T> type) {
        this.type = type;
    }

    public RulesExecutor<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                      @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
        RuleConfigReader<T> reader = new RuleConfigReader<T>()
                .env(env)
                .cache(cache)
                .contentDir(contentDir)
                .entityType(type);
        rules = reader.read(xmlConfig);
        return this;
    }

    public EvaluationStatus evaluate(@NonNull T input) throws Exception {
        synchronized (this) {
            if (evaluator == null) {
                evaluator = new RulesEvaluator<>(rules, terminateOnValidationError);
            }
        }
        EvaluationStatus status = new EvaluationStatus();
        status.setStatus(StatusCode.Success);
        evaluator.evaluate(input, status);
        if (status.getErrors() != null) {
            status.setStatus(StatusCode.ValidationFailed);
        }
        return status;
    }
}
