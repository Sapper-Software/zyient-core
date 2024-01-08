package io.zyient.core.mapping.readers.impl.db;

import io.zyient.core.persistence.AbstractDataStore;
import lombok.NonNull;

import java.util.Map;

public interface QueryBuilder {
    AbstractDataStore.Q build(@NonNull String query,
                              Map<String, Object> conditions) throws Exception;
}
