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

package io.zyient.base.core.decisions.impl;

import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.decisions.Condition;
import io.zyient.base.core.decisions.ConditionParser;
import io.zyient.base.core.decisions.Op;
import lombok.NonNull;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimpleMapConditionParser implements ConditionParser<Map<String, Object>> {
    private static final String PARSE_REGEX = "^(==|!=|<=|<|>=|>|~=|NULL|NOT NULL){1}+\\s*(.*)$";
    private static final Pattern PARSE_PATTERN = Pattern.compile(PARSE_REGEX);

    @Override
    public Condition<Map<String, Object>> parse(@NonNull String source,
                                                @NonNull String property,
                                                @NonNull Class<?> type,
                                                Context context) throws Exception {
        Matcher matcher = PARSE_PATTERN.matcher(source);
        if (matcher.matches()) {
            String op = matcher.group(1);
            String value = null;
            if (matcher.groupCount() > 1) {
                value = matcher.group(2);
                if (type.equals(String.class)) {
                    value = value.replaceAll("'", "")
                            .replaceAll("\"", "");
                }
            }
            Op operation = Op.from(op);
            if (operation == null) {
                throw new Exception(String.format("Failed to extract operation. [string=%s]", source));
            }
            if (ReflectionHelper.isNumericType(type)) {
                MapNumericCondition condition = (MapNumericCondition) new MapNumericCondition(type)
                        .property(property)
                        .op(operation);
                if (operation != Op.IsNull && operation != Op.NotNull) {
                    if (value == null || Strings.isNullOrEmpty(value)) {
                        throw new Exception(String.format("Failed to extract value. [string=%s]", source));
                    }
                } else {
                    value = op;
                }
                condition.value(Double.parseDouble(value));
                condition.validate();

                return condition;
            } else if (type.equals(String.class)) {
                MapStringCondition condition = (MapStringCondition) new MapStringCondition()
                        .property(property)
                        .op(operation);
                if (operation != Op.IsNull && operation != Op.NotNull) {
                    if (value == null || Strings.isNullOrEmpty(value)) {
                        throw new Exception(String.format("Failed to extract value. [string=%s]", source));
                    }
                } else {
                    value = op;
                }
                condition.value(value);
                condition.validate();

                return condition;
            }
        }
        return null;
    }
}
