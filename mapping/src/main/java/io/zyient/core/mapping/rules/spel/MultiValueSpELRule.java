package io.zyient.core.mapping.rules.spel;


import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.beans.PropertyDef;
import io.zyient.core.mapping.rules.MappingReflectionHelper;
import io.zyient.core.mapping.rules.Rule;
import io.zyient.core.mapping.rules.RuleConfig;
import io.zyient.core.mapping.rules.RuleType;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class MultiValueSpELRule<T> extends SpELRule<T> {
    @Override
    protected List<FieldMap> createTargetFields(RuleConfig config) throws Exception {
        List<FieldMap> fieldMaps = new ArrayList<>();
        Preconditions.checkArgument(config instanceof MultiValueSpELRuleConfig);
        MultiValueSpELRuleConfig spELRuleConfig = ((MultiValueSpELRuleConfig) config);
        if (spELRuleConfig.getFieldMappings() != null) {
            for (String fieldName : spELRuleConfig.getFieldMappings().keySet()) {
                String target = MappingReflectionHelper.normalizeField(fieldName);
                PropertyDef property = MappingReflectionHelper.findField(fieldName, entityType());
                if (property == null) {
                    throw new ConfigurationException(String.format("Failed to find property. [type=%s][property=%s]",
                            entityType().getCanonicalName(), fieldName));
                }
                fieldMaps.add(new FieldMap(target, spELRuleConfig.getFieldMappings().get(fieldName), property));
            }
        }
        return fieldMaps;
    }

    @Override
    protected void validate(RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof MultiValueSpELRuleConfig);
        MultiValueSpELRuleConfig spELRuleConfig = ((MultiValueSpELRuleConfig) config);
        if (getRuleType() == RuleType.Transformation) {
            if ((spELRuleConfig.getFieldMappings() == null || spELRuleConfig.getFieldMappings().isEmpty())) {
                throw new ConfigurationException(String.format("[rule=%s] FieldMappings not specified.", name()));
            }
        }
    }

    @Override
    public Rule<T> withContentDir(@NonNull File contentDir) {
        return this;
    }
}
