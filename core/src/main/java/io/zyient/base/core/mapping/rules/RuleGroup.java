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
import io.zyient.base.core.mapping.model.MappedResponse;
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
    private List<Rule<T>> rules;

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
        return this;
    }

    @Override
    public Rule<T> configure(@NonNull RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof RuleGroupConfig);
        settings = (RuleGroupConfig) config;
        return this;
    }

    @Override
    public Object evaluate(@NonNull MappedResponse<T> data) throws Exception {
        ValidationExceptions errors = null;
        try {
            Object response = null;
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

    @Override
    public RuleType getRuleType() {
        return RuleType.Group;
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
