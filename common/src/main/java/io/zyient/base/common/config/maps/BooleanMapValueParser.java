package io.zyient.base.common.config.maps;

import lombok.NonNull;

public class BooleanMapValueParser extends MapFieldValueParser<Boolean> {
    public BooleanMapValueParser() {
    }

    public BooleanMapValueParser(@NonNull String path,
                                 @NonNull String valuesPath,
                                 @NonNull String keyName,
                                 @NonNull String valueName) {
        super(path, valuesPath, keyName, valueName);
    }

    @Override
    protected Boolean fromString(@NonNull String value) throws Exception {
        return Boolean.parseBoolean(value);
    }
}
