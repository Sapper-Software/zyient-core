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

import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.mapping.model.RuleElement;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@Accessors(fluent = true)
public class RulesExecutor<T> {
    public static final String __CONFIG_PATH = "rules";
    public static final String FIELD_REGEX = "(\\{\\$.*?\\})";
    public static final String FIELD_ENTITY = "entity";
    public static final String FIELD_CUSTOM = "entity.properties";
    public static final String FIELD_SOURCE = "source";
    public static final String FIELD_CACHED = "cached";

    private final Class<? extends T> type;
    private final List<Rule> rules = new ArrayList<>();
    private ExpressionParser parser;
    private Pattern fieldFinder = Pattern.compile(FIELD_REGEX);

    public RulesExecutor(@NonNull Class<? extends T> type) {
        this.type = type;
    }

    public RulesExecutor<T> add(@NonNull RuleElement element) throws Exception {
        String r = element.getRule();
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
        Expression exp = parser.parseExpression(r);
        Field field = null;
        if (!element.isValidation()) {
            field = ReflectionUtils.findField(type, element.getTarget());
            if (field == null) {
                throw new ConfigurationException(String.format("Field not found. [type=%s][field=%s]",
                        type.getCanonicalName(), element.getTarget()));
            }
        }
        Rule rule = new Rule()
                .rule(r)
                .target(field)
                .expression(exp)
                .validation(element.isValidation());

        rules.add(rule);
        return this;
    }

    public RulesExecutor<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            SpelParserConfiguration config = new SpelParserConfiguration(true, true);
            parser = new SpelExpressionParser(config);

            ConfigPath cp = RuleElement.class.getAnnotation(ConfigPath.class);
            List<HierarchicalConfiguration<ImmutableNode>> nodes = xmlConfig.configurationsAt(cp.path());
            if (nodes == null || nodes.isEmpty()) {
                throw new ConfigurationException("No rules defined in configuration...");
            }
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                RuleElement re = RuleElement.read(node);
                add(re);
            }

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void evaluate(@NonNull MappedResponse<T> input) throws Exception {
        StandardEvaluationContext ctx = new StandardEvaluationContext(input);
        for (Rule rule : rules) {
            if (rule.validation()) {
                boolean r = (boolean) rule.expression().getValue(ctx);
                if (!r) {
                    if (DefaultLogger.isTraceEnabled()) {
                        String json = JSONUtils.asString(input, input.getClass());
                        throw new ValidationException(String.format("[rule=%s][data=%s]", rule.rule(), json));
                    } else
                        throw new ValidationException(String.format("[rule=%s]", rule.rule()));
                }
            } else {
                Object value = rule.expression().getValue(ctx);
                if (value != null) {
                    ReflectionUtils.setValue(value, input, rule.target());
                } else if (DefaultLogger.isTraceEnabled()) {
                    String json = JSONUtils.asString(input, input.getClass());
                    DefaultLogger.trace(String.format("Returned null : [rule=%s][data=%s]", rule.rule(), json));
                }
            }
        }
    }
}
