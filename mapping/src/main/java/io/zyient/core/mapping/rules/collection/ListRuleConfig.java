package io.zyient.core.mapping.rules.collection;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.core.mapping.rules.Rule;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ListRuleConfig extends  CollectionRuleConfig{

    @Config(name="isPresent")
    private String present;
    @Config(name="field")
    private String field;

    @Config(name = "items", required = false, custom = ListRuleReader.class)
    private List<String> items;

    @Override
    public <E> Rule<E> createInstance(@NonNull Class<? extends E> type) throws Exception {
        return new ListRule<>();
    }
}
