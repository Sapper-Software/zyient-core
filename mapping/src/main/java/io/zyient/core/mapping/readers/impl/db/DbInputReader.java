package io.zyient.core.mapping.readers.impl.db;

import com.google.common.base.Strings;
import io.zyient.base.common.model.PropertyModel;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.DbReaderSettings;
import io.zyient.core.mapping.rules.MappingReflectionHelper;
import io.zyient.core.mapping.rules.db.DBRule;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.env.DataStoreEnv;
import lombok.Getter;
import lombok.Setter;
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

    @Getter
    @Setter
    @Accessors(fluent = true)
    protected static class FieldProperty {
        private PropertyModel property;
        private String field;

        public FieldProperty(PropertyModel property, String field) {
            this.property = property;
            this.field = field;
        }
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
        Map<String, FieldProperty> whereFields = null;
        String query = settings.getQuery();
        Map<String, String> fs = MappingReflectionHelper.extractFields(query);
        if (!fs.isEmpty()) {
            whereFields = new HashMap<>();
            int index = 0;
            for (String key : fs.keySet()) {
                String f = fs.get(key);
                PropertyModel field = MappingReflectionHelper.findField(f, entityType());
                if (field == null) {
                    throw new Exception(String.format("Field not found. [entity=%s][field=%s]",
                            entityType().getCanonicalName(), f));
                }
                String param = String.format("param_%d", index);
                query = query.replace(key, ":" + param);
                f = MappingReflectionHelper.normalizeField(f);
                whereFields.put(param, new FieldProperty(field, f));
                index++;
            }
        }
        if (whereFields != null && !whereFields.isEmpty()) {
            Map<String, Object> params = new HashMap<>();
            for (String key : whereFields.keySet()) {
                FieldProperty field = whereFields.get(key);
                Object value = MappingReflectionHelper.getProperty(field.field(), field.property(), contentInfo().params);
                params.put(key, value);
            }
            AbstractDataStore.Q queryDataStore = builder.build(query, params);
            cursor = dataStore.search(queryDataStore,
                    0,
                    settings.getReadBatchSize(),
                    keyType,
                    entityType,
                    null);
        }

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
