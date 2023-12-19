package io.zyient.core.mapping.readers.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FlattenedInputReaderSettings extends SeparatedReaderSettings {
    @Config(name = "separator.section")
    private String sectionSeparator;
    @Config(name = "separator.field", required = false)
    private String fieldSeparator = ":";
}
