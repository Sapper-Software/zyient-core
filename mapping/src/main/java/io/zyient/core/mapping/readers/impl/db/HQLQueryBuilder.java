package io.zyient.core.mapping.readers.impl.db;

import io.zyient.core.persistence.AbstractDataStore;
import lombok.NonNull;

import java.util.Map;

public class HQLQueryBuilder implements QueryBuilder{
    @Override
    public AbstractDataStore.Q build(@NonNull String query, Map<String, Object> conditions) throws Exception {
        AbstractDataStore.Q q = null;
        if (conditions != null) {
            q = new AbstractDataStore.Q()
                    .where(query)
                    .addAll(conditions);
        } else {
            q = new AbstractDataStore.Q()
                    .where(query);
        }
        return q;
    }

}
