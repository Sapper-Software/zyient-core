/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.decisions;

import io.zyient.base.common.model.Context;
import io.zyient.base.core.decisions.Condition;
import io.zyient.base.core.decisions.impl.SimpleMapConditionParser;
import lombok.NonNull;

import java.util.Map;

public class MappingConditionParser extends SimpleMapConditionParser {
    public static final String CONTEXT_CONDITION_TYPE = "condition.type";

    @Override
    public Condition<Map<String, Object>> parse(@NonNull String source,
                                                String property,
                                                @NonNull Class<?> type,
                                                Context context) throws Exception {
        ConditionType t = type(context);
        if (t == ConditionType.Simple) {
            return super.parse(source, property, type, context);
        } else if (t == ConditionType.SpEL) {
            SpELCondition<Map<String, Object>> condition = new SpELCondition<>();
            condition.expressionString(source);
            condition.validate();

            return condition;
        }
        throw new Exception("Invalid Condition type...");
    }

    private ConditionType type(Context context) {
        ConditionType type = ConditionType.Simple;
        if (context != null && context.containsKey(CONTEXT_CONDITION_TYPE)) {
            type = (ConditionType) context.get(CONTEXT_CONDITION_TYPE);
        }
        return type;
    }
}
