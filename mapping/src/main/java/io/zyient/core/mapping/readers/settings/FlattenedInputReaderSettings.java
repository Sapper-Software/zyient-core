package io.zyient.core.mapping.readers.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FlattenedInputReaderSettings extends SeparatedReaderSettings {
    @Config(name = "separator.section")
    private String sectionSeparator;
    @Config(name = "separator.field", required = false)
    private String fieldSeparator = ":";
    @Config(name = HeaderMapSettingParser.__SECTION, custom = HeaderMapSettingParser.class)
    private Map<String, Boolean> fields;
}
