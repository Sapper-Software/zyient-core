package io.zyient.core.mapping.rules.collection;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.beans.PropertyDef;
import io.zyient.base.core.errors.Errors;
import io.zyient.core.mapping.rules.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;

@Getter
@Accessors(fluent = true)
public class ListRule<T> extends BaseRule<T> {

    private PropertyDef property;
    private String target;

    @Override
    protected Object doEvaluate(@NonNull T data) throws RuleValidationError, RuleEvaluationError {
        ListRuleConfig listRuleConfig = (ListRuleConfig) config();
        try {
            boolean isPresent = Boolean.parseBoolean(listRuleConfig.getPresent());
            Object value = MappingReflectionHelper.getProperty(target, property, data);
            if (value instanceof String s) {
                if (isPresent) {
                    if (listRuleConfig.getItems().contains(s)) {
                        return true;
                    } else {

                        throw new RuleValidationError(name(),
                                entityType(),
                                getRuleType().name(),
                                errorCode(),
                                Errors.getDefault().get(__ERROR_TYPE_VALIDATION, validationErrorCode()).getMessage()
                        );
                    }
                } else {
                    if (!listRuleConfig.getItems().contains(s)) {
                        return true;
                    } else {
                        throw new RuleValidationError(name(),
                                entityType(),
                                getRuleType().name(),
                                errorCode(),
                                Errors.getDefault().get(__ERROR_TYPE_VALIDATION, validationErrorCode()).getMessage()
                        );
                    }
                }
            }
        } catch (RuleValidationError | RuleEvaluationError e) {
            throw e;
        } catch (RuntimeException re) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    expression(),
                    errorCode(),
                    Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode()).getMessage(),
                    re);
        } catch (Throwable t) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    expression(),
                    errorCode(),
                    Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode()).getMessage(),
                    t);
        }

        return null;
    }

    @Override
    public void setup(@NonNull RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof ListRuleConfig);
        try {
            if (((ListRuleConfig) config).getField() != null) {
                property = MappingReflectionHelper.findField(((ListRuleConfig) config).getField(), entityType());
                if (property == null) {
                    throw new ConfigurationException(String.format("Failed to find property. [type=%s][field=%s]",
                            entityType().getCanonicalName(), ((ListRuleConfig) config).getField()));
                }
                target = MappingReflectionHelper.normalizeField(((ListRuleConfig) config).getField());
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Rule<T> withContentDir(@NonNull File contentDir) {
        return this;
    }
}
