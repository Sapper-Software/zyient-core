package io.zyient.base.common.config.maps;

import lombok.NonNull;

public class LongMapValueParser extends MapFieldValueParser<Long>{
    public LongMapValueParser() {
    }

    public LongMapValueParser(@NonNull String path,
                              @NonNull String valuesPath,
                              @NonNull String keyName,
                              @NonNull String valueName) {
        super(path, valuesPath, keyName, valueName);
    }

    @Override
    protected Long fromString(@NonNull String value) throws Exception {
        return Long.parseLong(value);
    }
}
