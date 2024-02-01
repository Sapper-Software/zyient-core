package io.zyient.core.mapping.rules.spel;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.core.mapping.rules.BaseRuleConfig;
import io.zyient.core.mapping.rules.Rule;
import io.zyient.core.mapping.rules.RuleType;
import io.zyient.core.mapping.rules.db.FieldMappingReader;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MultiValueSpELRuleConfig extends BaseRuleConfig {
    @Config(name = "fieldMappings", custom = FieldMappingReader.class)
    private Map<String, String> fieldMappings;

    @Override
    public void validate() throws ConfigurationException {
        super.validate();
        if (getType() == RuleType.Transformation) {
            if ((fieldMappings == null || fieldMappings.isEmpty())) {
                throw new ConfigurationException(String.format("Missing required property [fieldMappings]. [rule=%s]",
                        getName()));
            }
        }
    }

    public <E> Rule<E> createInstance(@NonNull Class<? extends E> entityType) throws Exception {
        return new MultiValueSpELRule<>();
    }
}
