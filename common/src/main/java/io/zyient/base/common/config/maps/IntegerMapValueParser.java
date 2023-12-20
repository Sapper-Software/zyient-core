package io.zyient.base.common.config.maps;

import lombok.NonNull;

public class IntegerMapValueParser extends MapFieldValueParser<Integer> {
    public IntegerMapValueParser() {
    }

    public IntegerMapValueParser(@NonNull String path,
                                 @NonNull String valuesPath,
                                 @NonNull String keyName,
                                 @NonNull String valueName) {
        super(path, valuesPath, keyName, valueName);
    }

    @Override
    protected Integer fromString(@NonNull String value) throws Exception {
        return Integer.parseInt(value);
    }
}
