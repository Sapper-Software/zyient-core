package io.zyient.core.mapping.readers.impl.db;

import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.DbReaderSettings;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.env.DataStoreEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class DbInputReader<K extends IKey, E extends IEntity<K>> extends InputReader {
    private AbstractDataStore<?> dataStore;
    private Class<? extends K> keyType;
    private Class<? extends E> entityType;
    private Cursor<K, E> cursor;
    private QueryBuilder builder;
    private Map<String, Object> predicates;

    public DbInputReader<K, E> withPredicates(@NonNull Map<String, Object> predicates) {
        this.predicates = predicates;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ReadCursor open() throws IOException {
        try {
            DbReaderSettings settings = (DbReaderSettings) settings();
            DataStoreEnv<?> env = BaseEnv.get(settings.getEnv(), DataStoreEnv.class);
            if (env == null) {
                throw new Exception(String.format("Data Store environment not found. [name=%s][type=%s]",
                        settings.getEnv(), DataStoreEnv.class.getCanonicalName()));
            }
            dataStore = env.getDataStoreManager()
                    .getDataStore(settings.getDataStore(), settings.getDataStoreType());
            if (dataStore == null) {
                throw new Exception(String.format("Data Store not found. [name=%s][type=%s]",
                        settings.getDataStore(), settings.getDataStoreType().getCanonicalName()));
            }
            if (Strings.isNullOrEmpty(settings.getQuery())) {
                throw new Exception("Filter query not specified...");
            }
            keyType = (Class<? extends K>) settings.getKeyType();
            entityType = (Class<? extends E>) settings.getEntityType();
            builder = settings.getBuilder()
                    .getDeclaredConstructor()
                    .newInstance();
            fetchQuery(settings);
            return new DbReadCursor<K, E>(this, settings().getReadBatchSize());
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void fetchQuery(DbReaderSettings settings) throws Exception {
        Map<String, Object> conditions = null;
        if (!Strings.isNullOrEmpty(settings.getCondition())) {
            conditions = JSONUtils.read(settings.getCondition(), Map.class);
            Map<String, Object> updated = new HashMap<>();
            for (String key : conditions.keySet()) {
                Object value = conditions.get(key);
                if (value instanceof String str) {
                    if (str.startsWith(":")) {
                        String name = str.substring(1);
                        if (predicates == null) {
                            throw new Exception(String.format("Predicate values expected for condition. [condition=%s]",
                                    settings.getCondition()));
                        }
                        value = predicates.get(key);
                        if (value == null) {
                            throw new Exception(String.format("Missing predicate value. [predicate=%s]", name));
                        }
                        updated.put(key, value);
                    }
                }
            }
            if (!updated.isEmpty()) {
                for (String key : updated.keySet()) {
                    conditions.put(key, updated.get(key));
                }
            }
        }
        AbstractDataStore.Q queryDataStore = builder.build(settings.getQuery(), conditions);
        cursor = dataStore.search(queryDataStore,
                0,
                settings.getReadBatchSize(),
                keyType,
                entityType,
                null);

    }

    @Override
    public List<SourceMap> nextBatch() throws IOException {
        try {
            List<E> data = cursor.nextPage();
            if (data != null && !data.isEmpty()) {
                List<SourceMap> values = new ArrayList<>(data.size());
                for (E entity : data) {
                    Map<String, Object> map = JSONUtils.asMap(entity);
                    SourceMap sourceMap = new SourceMap(map);
                    values.add(sourceMap);
                }
                return values;
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (cursor != null)
            cursor.close();
        cursor = null;
    }
}
