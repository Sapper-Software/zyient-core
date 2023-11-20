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
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
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


    @Override
    public void setup(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            normalizeRule();
            SpelParserConfiguration config = new SpelParserConfiguration(true, true);
            ExpressionParser parser = new SpelExpressionParser(config);
            expression = parser.parseExpression(getRule());
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void normalizeRule() throws Exception {
        String r = getRule();
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
        DefaultLogger.debug(String.format("[original=%s][normalized=%s]", getRule(), r));
        setRule(r);
    }

    @Override
    public Object doEvaluate(@NonNull MappedResponse<T> data) throws Exception {
        StandardEvaluationContext ctx = new StandardEvaluationContext(data);
        Object response = expression.getValue(ctx);
        if (getRuleType() == RuleType.Validation || getRuleType() == RuleType.Condition) {
            if (!(response instanceof Boolean)) {
                throw new Exception(String.format("Failed to execute rule. [rule=%s]", getRule()));
            }
            boolean r = (boolean) response;
            if (!r) {
                if (getRuleType() == RuleType.Validation) {
                    if (DefaultLogger.isTraceEnabled()) {
                        String json = JSONUtils.asString(data, data.getClass());
                        throw new ValidationException(String.format("[rule=%s][data=%s]", getRule(), json));
                    } else
                        throw new ValidationException(String.format("[rule=%s]", getRule()));
                } else {
                    if (DefaultLogger.isTraceEnabled()) {
                        String json = JSONUtils.asString(data, data.getClass());
                        throw new RuleConditionFailed(getRule(), json);
                    } else
                        throw new RuleConditionFailed(getRule());
                }
            }
        } else if (response != null) {
            ReflectionUtils.setValue(response, data, getTargetField());
        } else if (DefaultLogger.isTraceEnabled()) {
            String json = JSONUtils.asString(data, data.getClass());
            DefaultLogger.trace(String.format("Returned null : [rule=%s][data=%s]", getRule(), json));
        }
        return response;
    }
}
