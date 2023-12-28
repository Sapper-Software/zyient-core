package io.zyient.core.mapping.rules.collection;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.PropertyModel;
import io.zyient.base.core.errors.Errors;
import io.zyient.core.mapping.rules.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class ReplaceRule<T> extends BaseRule<T> {

    private PropertyModel sourceProperty;
    private PropertyModel targetProperty;
    private String target;
    private String field;

    @Override
    protected Object doEvaluate(@NonNull T data) throws RuleValidationError, RuleEvaluationError {
        ReplaceRuleConfig replaceRuleConfig = (ReplaceRuleConfig) config();
        try {
            Map<String, String> replaceMap = replaceRuleConfig.getFieldMappings();
            Object value = MappingReflectionHelper.getProperty(field, sourceProperty, data);
            if (value instanceof String s) {
                for (String key : replaceMap.keySet()) {
                    s = s.replace(key, replaceMap.get(key));
                }
                MappingReflectionHelper.setProperty(target, targetProperty, data, s);
            }

        } catch (RuleValidationError | RuleEvaluationError e) {
            throw e;
        } catch (Exception re) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    expression(),
                    errorCode(),
                    Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode()).getMessage(),
                    re);
        }

        return null;
    }

    @Override
    public void setup(@NonNull RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof ReplaceRuleConfig);
        try {
            if (((ReplaceRuleConfig) config).getField() != null) {
                sourceProperty = MappingReflectionHelper.findField(((ReplaceRuleConfig) config).getField(), entityType());
                if (sourceProperty == null) {
                    throw new ConfigurationException(String.format("Failed to find property. [type=%s][field=%s]",
                            entityType().getCanonicalName(), ((ReplaceRuleConfig) config).getField()));
                }
                field = MappingReflectionHelper.normalizeField(((ReplaceRuleConfig) config).getField());
            }
            if (((ReplaceRuleConfig) config).getTarget() != null) {
                targetProperty = MappingReflectionHelper.findField(((ReplaceRuleConfig) config).getTarget(), entityType());
                if (targetProperty == null) {
                    throw new ConfigurationException(String.format("Failed to find property. [type=%s][field=%s]",
                            entityType().getCanonicalName(), ((ReplaceRuleConfig) config).getTarget()));
                }
                target = MappingReflectionHelper.normalizeField(((ReplaceRuleConfig) config).getTarget());
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
