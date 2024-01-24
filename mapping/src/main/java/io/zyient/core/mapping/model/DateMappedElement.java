package io.zyient.core.mapping.model;

import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;
import io.zyient.core.mapping.model.mapping.CustomMappedElement;

@Getter
@Setter
public class DateMappedElement extends CustomMappedElement {
    @Config(name = "transformer.sourceFormat", required = true)
    private String sourceFormat;
    @Config(name = "transformer.targetFormat", required = true)
    private String targetFormat;
}
