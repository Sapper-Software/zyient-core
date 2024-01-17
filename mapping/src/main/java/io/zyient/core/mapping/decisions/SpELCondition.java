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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.core.decisions.Condition;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

@Getter
@Setter
@Accessors(fluent = true)
public class SpELCondition<T> implements Condition<T> {
    private String expressionString;
    @Setter(AccessLevel.NONE)
    private Expression expression;

    @Override
    public boolean evaluate(@NonNull T data) throws Exception {
        Preconditions.checkNotNull(expression);
        StandardEvaluationContext ctx = new StandardEvaluationContext(data);
        return Boolean.TRUE.equals(expression.getValue(ctx, Boolean.class));
    }

    @Override
    public void validate() throws ValidationException {
        if (Strings.isNullOrEmpty(expressionString))
            throw new ValidationException("SpEL expression is missing...");
        ExpressionParser parser = new SpelExpressionParser();
        expression = parser.parseExpression(expressionString);
    }
}
