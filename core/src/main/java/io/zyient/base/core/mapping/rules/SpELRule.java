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

import io.zyient.base.common.config.Config;
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

import java.lang.reflect.Field;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
public class SpELRule<T> implements Rule<T> {
    public static final String FIELD_REGEX = "(\\{\\$.*?\\})";
    public static final String FIELD_ENTITY = "entity";
    public static final String FIELD_CUSTOM = "entity.properties";
    public static final String FIELD_SOURCE = "source";
    public static final String FIELD_CACHED = "cached";
    @Config(name = "rule")
    private String rule;
    @Config(name = "target", required = false)
    private String target = null;
    @Setter(AccessLevel.NONE)
    private Expression expression;
    @Config(name = "validation", required = false)
    private boolean validation = false;
    private Field targetField;
    private Class<? extends T> type;

    @Override
    public Rule<T> withTargetField(Field targetField) throws Exception {
        if (!validation) {
            if (targetField == null) {
                throw new Exception(String.format("Target field required for rule. [rule=%s]", rule));
            }
        }
        this.targetField = targetField;
        return this;
    }

    @Override
    public Rule<T> withType(@NonNull Class<? extends T> type) {
        this.type = type;
        return this;
    }

    @Override
    public Rule<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        if (type == null) {
            throw new ConfigurationException("Entity type not specified...");
        }
        if (!validation) {
            if (targetField == null) {
                throw new ConfigurationException(String.format("Target field not set. [target=%s]", target));
            }
        }
        try {
            normalizeRule();
            SpelParserConfiguration config = new SpelParserConfiguration(true, true);
            ExpressionParser parser = new SpelExpressionParser(config);
            expression = parser.parseExpression(rule);
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void normalizeRule() throws Exception {
        String r = rule;
        Pattern fieldFinder = Pattern.compile(FIELD_REGEX);
        Matcher m = fieldFinder.matcher(r);
        if (m.matches()) {
            for (int ii = 0; ii < m.groupCount(); ii++) {
                String f = m.group(ii);
                String mf = f.replaceAll("[\\$\\{\\}]", "");
                if (mf.startsWith("custom.")) {
                    mf = mf.replace("custom\\.", FIELD_CUSTOM);
                } if (!mf.startsWith(FIELD_SOURCE) &&
                        !mf.startsWith(FIELD_ENTITY) &&
                        !mf.startsWith(FIELD_CACHED)) {
                    mf = FIELD_ENTITY + "." + mf;
                }
                mf = "#root." + mf;
                r = r.replace(f, mf);
            }
        }
        DefaultLogger.debug(String.format("[original=%s][normalized=%s]", rule, r));
        rule = r;
    }

    @Override
    public Object evaluate(@NonNull MappedResponse<T> data) throws Exception {
        StandardEvaluationContext ctx = new StandardEvaluationContext(data);
        Object response = expression.getValue(ctx);
        if (validation) {
            if (!(response instanceof Boolean)) {
                throw new Exception(String.format("Failed to execute rule. [rule=%s]", rule));
            }
            boolean r = (boolean) response;
            if (!r) {
                if (DefaultLogger.isTraceEnabled()) {
                    String json = JSONUtils.asString(data, data.getClass());
                    throw new ValidationException(String.format("[rule=%s][data=%s]", rule, json));
                } else
                    throw new ValidationException(String.format("[rule=%s]", rule));
            }
        } else if (response != null) {
            ReflectionUtils.setValue(response, data, targetField);
        } else if (DefaultLogger.isTraceEnabled()) {
            String json = JSONUtils.asString(data, data.getClass());
            DefaultLogger.trace(String.format("Returned null : [rule=%s][data=%s]", rule, json));
        }
        return response;
    }
}
