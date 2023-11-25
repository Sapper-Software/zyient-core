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
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.lang.reflect.Field;
import java.util.List;


public interface Rule<T> {
    String __RULE_TYPE = "rules";

    String name();

    List<String> getTargets();

    Rule<T> withTargetField(Field targetField) throws Exception;

    Rule<T> withEntityType(@NonNull Class<? extends T> type);

    Rule<T> configure(@NonNull RuleConfig config) throws ConfigurationException;

    Object evaluate(@NonNull MappedResponse<T> data) throws Exception;

    RuleType getRuleType();

    void addSubRules(@NonNull List<Rule<T>> rules);
}
