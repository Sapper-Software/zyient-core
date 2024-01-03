package io.zyient.core.mapping.rules.collection;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.core.mapping.rules.Rule;
import io.zyient.core.mapping.rules.db.FieldMappingReader;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ReplaceRuleConfig extends  CollectionRuleConfig{

    @Config(name = "fieldMappings", required = false, custom = FieldMappingReader.class)
    private Map<String, String> fieldMappings;

    @Config(name="field")
    private String field;
    @Config(name="target")
    private String target;


    @Override
    public <E> Rule<E> createInstance(@NonNull Class<? extends E> type) throws Exception {
        return new ReplaceRule<>();
    }
}
