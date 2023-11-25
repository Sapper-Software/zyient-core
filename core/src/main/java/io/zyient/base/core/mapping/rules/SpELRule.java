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

import io.zyient.base.common.errors.Error;
import io.zyient.base.common.errors.Errors;
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

import java.lang.reflect.Field;
import java.util.Map;

@Getter
@Setter
public class SpELRule<T> extends BaseRule<T> {
    public static final String FIELD_ROOT = "#root";

    @Setter(AccessLevel.NONE)
    private Expression expression;
    private Field targetField;

    private void normalizeRule() throws Exception {
        String r = rule();
        Map<String, String> fields = MappingReflectionHelper.extractFields(r);
        if (!fields.isEmpty()) {
            for (String exp : fields.keySet()) {
                String var = fields.get(exp);
                var = String.format("%s.%s", FIELD_ROOT, var);
                r = r.replace(exp, var);
            }
            DefaultLogger.debug(String.format("[original=%s][normalized=%s]", rule(), r));
            rule(r);
        }
    }


    @Override
    public Object doEvaluate(@NonNull MappedResponse<T> data) throws RuleValidationError, RuleEvaluationError {
        StandardEvaluationContext ctx = new StandardEvaluationContext(data);
        try {
            Object response = expression.getValue(ctx);
            if (getRuleType() == RuleType.Validation ||
                    getRuleType() == RuleType.Condition) {
                if (!(response instanceof Boolean)) {
                    if (response == null) {
                        throw new RuleEvaluationError(name(),
                                entityType(),
                                targetFieldString(),
                                errorCode(),
                                "NULL response from rule."
                        );

                    } else {
                        throw new RuleEvaluationError(name(),
                                entityType(),
                                targetFieldString(),
                                errorCode(),
                                String.format("Expected boolean response. [response=%s]",
                                        response.getClass().getCanonicalName())
                        );
                    }
                }
                boolean r = (boolean) response;
                if (!r) {
                    if (getRuleType() == RuleType.Validation) {
                        if (DefaultLogger.isTraceEnabled()) {
                            String json = JSONUtils.asString(data, data.getClass());
                            throw new RuleValidationError(name(),
                                    entityType(),
                                    targetFieldString(),
                                    errorCode(),
                                    Errors.getDefault().get(__RULE_TYPE, validationErrorCode()).getMessage(),
                                    new Exception(json)
                            );
                        } else
                            throw new RuleValidationError(name(),
                                    entityType(),
                                    targetFieldString(),
                                    errorCode(),
                                    Errors.getDefault().get(__RULE_TYPE, validationErrorCode()).getMessage()
                            );
                    } else {
                        if (DefaultLogger.isTraceEnabled()) {
                            DefaultLogger.trace(rule(), data);
                        }
                        return false;
                    }
                }
            } else if (response != null) {
                ReflectionUtils.setValue(response, data, targetField);
            } else if (DefaultLogger.isTraceEnabled()) {
                String json = JSONUtils.asString(data, data.getClass());
                DefaultLogger.trace(String.format("Returned null : [rule=%s][data=%s]", rules(), json));
            }
            return response;
        } catch (RuleValidationError | RuleEvaluationError e) {
            throw e;
        } catch (RuntimeException re) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    targetFieldString(),
                    errorCode(),
                    Errors.getDefault().get(__RULE_TYPE, errorCode()).getMessage(),
                    re);
        } catch (Throwable t) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    targetFieldString(),
                    errorCode(),
                    Errors.getDefault().get(__RULE_TYPE, errorCode()).getMessage(),
                    t);
        }
    }

    @Override
    protected void setup(@NonNull RuleConfig config) throws ConfigurationException {
        try {
            if (targetFields() != null) {
                if (targetFields().size() > 1) {
                    throw new ConfigurationException(
                            String.format("[rule=%s] Multiple targets not supported...", name()));
                }
                for (Field field : targetFields().values()) {
                    targetField = field;
                }
            }
            Error error = Errors.getDefault().get(__RULE_TYPE, errorCode());
            if (error == null) {
                throw new Exception(String.format("Invalid Error code: [code=%d]", errorCode()));
            }
            if (validationErrorCode() > 0) {
                error = Errors.getDefault().get(__RULE_TYPE, validationErrorCode());
                if (error == null) {
                    throw new Exception(String.format("Invalid Validation Error code: [code=%d]", validationErrorCode()));
                }
            }
            normalizeRule();
            SpelParserConfiguration cfg = new SpelParserConfiguration(true, true);
            ExpressionParser parser = new SpelExpressionParser(cfg);
            expression = parser.parseExpression(rule());
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
