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

package io.zyient.core.mapping.rules.spel;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.core.mapping.rules.BaseRuleConfig;
import io.zyient.core.mapping.rules.Rule;
import io.zyient.core.mapping.rules.RuleType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SpELRuleConfig extends BaseRuleConfig {
    @Config(name = "field", required = false)
    private String target;


    @Override
    public void validate() throws ConfigurationException {
        super.validate();
        if (getType() == RuleType.Transformation) {
            if (Strings.isNullOrEmpty(target)) {
                throw new ConfigurationException(String.format("Missing required property [target]. [rule=%s]",
                        getName()));
            }
        }
    }

    public <E> Rule<E> createInstance(@NonNull Class<? extends E> entityType) throws Exception {
        return new SpELRule<E>();
    }
}
