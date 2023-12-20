package io.zyient.core.mapping.readers.settings;

import io.zyient.base.common.config.maps.BooleanMapValueParser;

public class HeaderMapSettingParser extends BooleanMapValueParser {
    public static final String __SECTION = "section.header";
    public static final String CONFIG_FIELDS = "fields";
    public static final String CONFIG_FIELD = "field";
    public static final String CONFIG_REQUIRED = "required";

    public HeaderMapSettingParser() {
        super(__SECTION, CONFIG_FIELDS, CONFIG_FIELD, CONFIG_REQUIRED);
    }
}
