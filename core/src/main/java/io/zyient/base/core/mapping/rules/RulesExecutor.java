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

import io.zyient.base.core.mapping.model.MappedResponse;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class RulesExecutor<T> {
    private final Class<? extends T> type;
    private final List<Rule<T>> rules = new ArrayList<>();

    public RulesExecutor(@NonNull Class<? extends T> type) {
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    public RulesExecutor<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            List<HierarchicalConfiguration<ImmutableNode>> nodes = xmlConfig.configurationsAt(Rule.__CONFIG_PATH_RULE);
            if (nodes == null || nodes.isEmpty()) {
                throw new ConfigurationException("No rules defined in configuration...");
            }
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                Rule<T> rule = BaseRule.read(node, type);
                rules.add(rule);
            }

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
    public void evaluate(@NonNull MappedResponse<T> input) throws Exception {
        for (Rule<T> rule : rules) {
            rule.evaluate(input);
        }
    }
}
