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

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.StatusCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class FilterChain<T> {
    public static final String __CONFIG_PATH = "filters";

    private final Class<? extends T> type;
    private List<Rule<T>> chain;
    private File contentDir;

    public FilterChain(Class<? extends T> type) {
        this.type = type;
    }

    public FilterChain<T> withContentDir(@NonNull File contentDir) {
        this.contentDir = contentDir;
        return this;
    }

    public FilterChain<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                    @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            RuleConfigReader<T> reader = new RuleConfigReader<T>()
                    .env(env)
                    .contentDir(contentDir)
                    .entityType(type);
            chain = reader.read(xmlConfig);
            for (Rule<T> rule : chain) {
                validate(rule);
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    private void validate(Rule<T> rule) throws Exception {
        if (rule.getRuleType() == RuleType.Filter) {
            return;
        } else if (rule.getRuleType() == RuleType.Group) {
            RuleGroup<T> group = (RuleGroup<T>) rule;
            for (Rule<T> r : group.rules()) {
                validate(r);
            }
        }
        throw new Exception(String.format("Rule not supported in filters. [name=%s][type=%s]",
                rule.name(), rule.getRuleType().name()));
    }

    public StatusCode evaluate(@NonNull T data) throws RuleEvaluationError {
        for (Rule<T> rule : chain) {
            try {
                EvaluationStatus status = rule.evaluate(data);
                if (status.getStatus() == StatusCode.IgnoreRecord) {
                    return StatusCode.IgnoreRecord;
                }
            } catch (RuleValidationError ve) {
                throw new RuleEvaluationError(rule.name(),
                        rule.entityType(),
                        rule.getRuleType().name(),
                        rule.errorCode(),
                        "Invalid rule...",
                        ve);
            }
        }
        return StatusCode.Success;
    }
}
