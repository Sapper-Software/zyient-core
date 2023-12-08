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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class BaseRuleConfig extends RuleConfig {
    @Config(name = "expression", required = false)
    private String expression;
    @Config(name = "errorCode", required = false, type = Integer.class)
    private Integer errorCode;
    @Config(name = "validationErrorCode", required = false, type = Integer.class)
    private Integer validationErrorCode;

    public void validate() throws ConfigurationException {
        if (getType() == RuleType.Group || getType() == RuleType.Reference) {
            throw new ConfigurationException(String.format("Invalid Rule type. [type=%s][rule=%s]",
                    getType().name(), getName()));
        }
        if (errorCode == null) {
            throw new ConfigurationException(String.format("Missing required property [errorCode]. [rule=%s]",
                    getName()));
        }
        if (Strings.isNullOrEmpty(expression)) {
            throw new ConfigurationException(String.format("Missing required property [expression]. [rule=%s]",
                    getName()));
        }
        if (getType() == RuleType.Validation) {
            if (validationErrorCode == null) {
                throw new ConfigurationException(
                        String.format("Missing required property [validationErrorCode]. [rule=%s]", getName()));
            }
        }
    }
}
