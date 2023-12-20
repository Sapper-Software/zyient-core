package io.zyient.base.common.config.maps;

import lombok.NonNull;

public class StringMapValueParser extends MapFieldValueParser<String>{
    public StringMapValueParser() {
    }

    public StringMapValueParser(@NonNull String path,
                                @NonNull String valuesPath,
                                @NonNull String keyName,
                                @NonNull String valueName) {
        super(path, valuesPath, keyName, valueName);
    }

    @Override
    protected String fromString(@NonNull String value) throws Exception {
        return value.trim();
    }
}
