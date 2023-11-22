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

import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RulesCache<T> {
    private Class<? extends T> type;
    private final Map<String, Rule<T>> cache = new HashMap<>();

    public boolean has(@NonNull String name) {
        return cache.containsKey(name);
    }

    public Rule<T> get(@NonNull String name) {
        return cache.get(name);
    }

    @SuppressWarnings("unchecked")
    public RulesCache<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> node,
                                   Class<?> type) throws ConfigurationException {
        this.type = (Class<? extends T>) type;
        try {
            RuleConfigReader<T> reader = new RuleConfigReader<T>()
                    .cache(this)
                    .entityType(this.type);
            List<Rule<T>> rules = reader.read(node);
            if (rules != null && !rules.isEmpty()) {
                for (Rule<T> rule : rules) {
                    cache.put(rule.name(), rule);
                }
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
