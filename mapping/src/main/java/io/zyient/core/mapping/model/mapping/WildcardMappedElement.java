package io.zyient.core.mapping.model.mapping;

import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WildcardMappedElement extends MappedElement {
    @Config(name = "prefix", required = false)
    private String prefix = "VAL";
}
