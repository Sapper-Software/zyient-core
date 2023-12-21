package io.zyient.base.common.config.maps;

import lombok.NonNull;

public class DoubleMapValueParser extends MapFieldValueParser<Double> {
    public DoubleMapValueParser() {
    }

    public DoubleMapValueParser(@NonNull String path,
                                @NonNull String valuesPath,
                                @NonNull String keyName,
                                @NonNull String valueName) {
        super(path, valuesPath, keyName, valueName);
    }

    @Override
    protected Double fromString(@NonNull String value) throws Exception {
        return Double.parseDouble(value);
    }
}
