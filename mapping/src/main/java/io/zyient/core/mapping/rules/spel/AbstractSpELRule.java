package io.zyient.core.mapping.rules.spel;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.beans.PropertyDef;
import io.zyient.base.core.errors.Error;
import io.zyient.base.core.errors.Errors;
import io.zyient.core.mapping.rules.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.File;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class AbstractSpELRule<T> extends BaseRule<T> {
    public static final String FIELD_ROOT = "#root";
    public static final String FIELD_RESULT = "__zy_result";

    protected Expression spELRule;
    private List<FieldMap> fieldMaps;

    @AllArgsConstructor
    protected static class FieldMap {
        String targetField;
        String targetValue;
        PropertyDef property;
    }

    private void normalizeRule() throws Exception {
        String r = expression();
        Map<String, String> fields = MappingReflectionHelper.extractFields(r);
        if (fields != null && !fields.isEmpty()) {
            for (String exp : fields.keySet()) {
                String var = fields.get(exp);
                if (MappingReflectionHelper.isPropertyPrefixed(var)
                        || MappingReflectionHelper.isEntityPropertyPrefixed(var)) {
                    var = MappingReflectionHelper.fieldToPropertyGetMethod(var);
                }
                var = String.format("%s.%s", FIELD_ROOT, var);
                r = r.replace(exp, var);
            }
            DefaultLogger.debug(String.format("[original=%s][normalized=%s]", expression(), r));
            expression(r);
        }
        if (getRuleType() == RuleType.Transformation) {
            r = expression();
            r = String.format("#%s=(%s)", FIELD_RESULT, r);
            expression(r);
        }
    }

    @Override
    public Object doEvaluate(@NonNull T data) throws RuleValidationError, RuleEvaluationError {
        StandardEvaluationContext ctx = new StandardEvaluationContext(data);
        Object result = null;
        ctx.setVariable(FIELD_RESULT, result);
        try {
            return validateResponse(spELRule.getValue(ctx), data);
        } catch (RuleValidationError | RuleEvaluationError e) {
            throw e;
        } catch (Throwable t) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    expression(),
                    errorCode(),
                    Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode()).getMessage(),
                    t);
        }
    }

    protected Object validateResponse(Object response, T data) throws Exception {
        if (getRuleType() == RuleType.Validation ||
                getRuleType() == RuleType.Condition ||
                getRuleType() == RuleType.Filter) {
            if (!(response instanceof Boolean)) {
                if (response == null) {
                    throw new RuleEvaluationError(name(),
                            entityType(),
                            getRuleType().name(),
                            errorCode(),
                            "NULL response from rule."
                    );

                } else {
                    throw new RuleEvaluationError(name(),
                            entityType(),
                            getRuleType().name(),
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
                        String json = JSONUtils.asString(data);
                        throw new RuleValidationError(name(),
                                entityType(),
                                getRuleType().name(),
                                errorCode(),
                                Errors.getDefault().get(__ERROR_TYPE_VALIDATION, validationErrorCode()).getMessage(),
                                new Exception(json)
                        );
                    } else
                        throw new RuleValidationError(name(),
                                entityType(),
                                getRuleType().name(),
                                errorCode(),
                                Errors.getDefault().get(__ERROR_TYPE_VALIDATION, validationErrorCode()).getMessage()
                        );
                } else {
                    if (DefaultLogger.isTraceEnabled()) {
                        DefaultLogger.trace(expression(), data);
                    }
                    return false;
                }
            }
        } else if (response != null) {
            /**
             * normal SpEL will set the output of the SpEL to target
             */
            if (fieldMaps.size() == 1) {
                // set SpeL output to target
                MappingReflectionHelper.setProperty(fieldMaps.get(0).targetField, fieldMaps.get(0).property, data, response);
            } else {
                for (FieldMap fieldMap : fieldMaps) {
                    MappingReflectionHelper.setProperty(fieldMap.targetField, fieldMap.property, data, fieldMap.targetValue);
                }
            }
        } else if (DefaultLogger.isTraceEnabled()) {
            String json = JSONUtils.asString(data);
            DefaultLogger.trace(String.format("Returned null : [rule=%s][data=%s]", rules(), json));
        }
        return response;
    }

    @Override
    public void setup(@NonNull RuleConfig config) throws ConfigurationException {
        try {
            this.validate(config);
            fieldMaps = createTargetFields(config);

            if (getRuleType() == RuleType.Transformation && fieldMaps.isEmpty()) {
                throw new ConfigurationException(String.format("[target] is required. [type=%s]",
                        entityType().getCanonicalName()));
            }

            Error error = Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode());
            if (error == null) {
                throw new Exception(String.format("Invalid Error code: [code=%d]", errorCode()));
            }
            if (validationErrorCode() > 0) {
                error = Errors.getDefault().get(__ERROR_TYPE_VALIDATION, validationErrorCode());
                if (error == null) {
                    throw new Exception(String.format("Invalid Validation Error code: [code=%d]", validationErrorCode()));
                }
            }
            normalizeRule();
            SpelParserConfiguration cfg = new SpelParserConfiguration(true, true);
            ExpressionParser parser = new SpelExpressionParser(cfg);
            spELRule = parser.parseExpression(expression());
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    protected abstract List<FieldMap> createTargetFields(RuleConfig config) throws Exception;

    protected abstract void validate(RuleConfig config) throws ConfigurationException;

    @Override
    public Rule<T> withContentDir(@NonNull File contentDir) {
        return this;
    }

}
