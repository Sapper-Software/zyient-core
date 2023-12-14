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

import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.core.mapping.model.MappedResponse;
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

    public RulesExecutor(@NonNull Class<? extends T> type) {
        this.type = type;
    }

    public RulesExecutor<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        RuleConfigReader<T> reader = new RuleConfigReader<T>()
                .cache(cache)
                .contentDir(contentDir)
                .entityType(type);
        rules = reader.read(xmlConfig);
        return this;
    }

    public RulesEvaluationStatus evaluate(@NonNull MappedResponse<T> input,
                                          boolean terminateOnValidationError) throws Exception {
        try {
            for (Rule<T> rule : rules) {
                Object r = rule.evaluate(input);
                if (rule.getRuleType() == RuleType.Validation) {
                    if (rule.ignoreRecordOnCondition()) {
                        if (!(r instanceof Boolean)) {
                            throw new Exception(String.format("Invalid Rule response. [rule=%s][type=%s]",
                                    rule.name(), r.getClass().getCanonicalName()));
                        }
                        boolean ret = (boolean) r;
                        if (ret) return RulesEvaluationStatus.IgnoreRecord;
                    }
                }
            }
            if (input.errors() != null) {
                return RulesEvaluationStatus.ValidationFailed;
            }
            return RulesEvaluationStatus.Success;
        } catch (ValidationException ex) {
            if (terminateOnValidationError)
                throw ex;
            input.errors(ValidationExceptions.add(ex, input.errors()));
            return RulesEvaluationStatus.ValidationFailed;
        } catch (ValidationExceptions ex) {
            if (terminateOnValidationError)
                throw ex;
            input.errors(ex);
            return RulesEvaluationStatus.ValidationFailed;
        }
    }
}
