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

import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.model.MappedResponse;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
public class SpELRule<T> extends BaseRule<T> {
    public static final String FIELD_REGEX = "(\\{\\$.*?\\})";
    public static final String FIELD_ENTITY = "entity";
    public static final String FIELD_CUSTOM = "entity.properties";
    public static final String FIELD_SOURCE = "source";
    public static final String FIELD_CACHED = "cached";
    @Setter(AccessLevel.NONE)
    private Expression expression;

    private void normalizeRule() throws Exception {
        String r = rule();
        Pattern fieldFinder = Pattern.compile(FIELD_REGEX);
        Matcher m = fieldFinder.matcher(r);
        if (m.matches()) {
            for (int ii = 0; ii < m.groupCount(); ii++) {
                String f = m.group(ii);
                String mf = f.replaceAll("[\\$\\{\\}]", "");
                if (mf.startsWith("custom.")) {
                    mf = mf.replace("custom\\.", FIELD_CUSTOM);
                }
                if (!mf.startsWith(FIELD_SOURCE) &&
                        !mf.startsWith(FIELD_ENTITY) &&
                        !mf.startsWith(FIELD_CACHED)) {
                    mf = FIELD_ENTITY + "." + mf;
                }
                mf = "#root." + mf;
                r = r.replace(f, mf);
            }
        }
        DefaultLogger.debug(String.format("[original=%s][normalized=%s]", rule(), r));
        rule(r);
    }

    @Override
    public Object doEvaluate(@NonNull MappedResponse<T> data) throws Exception {
        StandardEvaluationContext ctx = new StandardEvaluationContext(data);
        Object response = expression.getValue(ctx);
        if (getRuleType() == RuleType.Validation || getRuleType() == RuleType.Condition) {
            if (!(response instanceof Boolean)) {
                throw new Exception(String.format("Failed to execute rule. [rule=%s]", rule()));
            }
            boolean r = (boolean) response;
            if (!r) {
                if (getRuleType() == RuleType.Validation) {
                    if (DefaultLogger.isTraceEnabled()) {
                        String json = JSONUtils.asString(data, data.getClass());
                        throw new ValidationException(String.format("[rule=%s][data=%s]", rule(), json));
                    } else
                        throw new ValidationException(String.format("[rule=%s]", rule()));
                } else {
                    if (DefaultLogger.isTraceEnabled()) {
                        String json = JSONUtils.asString(data, data.getClass());
                        throw new RuleConditionFailed(rule(), json);
                    } else
                        throw new RuleConditionFailed(rule());
                }
            }
        } else if (response != null) {
            ReflectionUtils.setValue(response, data, targetField());
        } else if (DefaultLogger.isTraceEnabled()) {
            String json = JSONUtils.asString(data, data.getClass());
            DefaultLogger.trace(String.format("Returned null : [rule=%s][data=%s]", rules(), json));
        }
        return response;
    }

    @Override
    protected void setup(@NonNull RuleConfig config) throws ConfigurationException {
        try {
            normalizeRule();
            SpelParserConfiguration cfg = new SpelParserConfiguration(true, true);
            ExpressionParser parser = new SpelExpressionParser(cfg);
            expression = parser.parseExpression(rule());
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
