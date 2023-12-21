package io.zyient.core.mapping.rules.db;

import io.zyient.base.common.config.maps.StringMapValueParser;

public class FieldMappingReader extends StringMapValueParser {
    public static final String __CONFIG_PATH = "fieldMappings";
    public static final String CONFIG_NODE = "mapping";
    public static final String CONFIG_SOURCE = "source";
    public static final String CONFIG_TARGET = "target";

    public FieldMappingReader() {
        super(__CONFIG_PATH, CONFIG_NODE, CONFIG_SOURCE, CONFIG_TARGET);
    }
}
