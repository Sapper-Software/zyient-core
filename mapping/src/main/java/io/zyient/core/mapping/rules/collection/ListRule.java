package io.zyient.core.mapping.rules.collection;

import com.google.common.base.Preconditions;
import io.zyient.base.core.errors.Errors;
import io.zyient.core.mapping.model.RuleDef;
import io.zyient.core.mapping.rules.*;
import io.zyient.core.mapping.rules.spel.AbstractSpELRule;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.List;


@Getter
@Accessors(fluent = true)
public class ListRule<T> extends AbstractSpELRule<T> {


    @Override
    protected Object validateResponse(Object response, T data) {
        return response;
    }

    @Override
    public Object doEvaluate(@NonNull T data) throws RuleValidationError, RuleEvaluationError {
        ListRuleConfig listRuleConfig = (ListRuleConfig) config();

        try {
            Object value = super.doEvaluate(data);
            boolean isPresent = Boolean.parseBoolean(listRuleConfig.getPresent());
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
    protected List<FieldMap> createTargetFields(RuleConfig config) throws Exception {
        return null;
    }

    @Override
    protected void validate(RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof ListRuleConfig);

        if (!(ruleType() == RuleType.Validation
                || ruleType() == RuleType.Filter
                || ruleType() == RuleType.Condition)) {
            throw new ConfigurationException(String.format("RuleType [%s] is not supported. [type=%s]", ruleType().name(),
                    entityType().getCanonicalName()));
        }
    }

    @Override
    public Rule<T> withContentDir(@NonNull File contentDir) {
        return this;
    }

}
