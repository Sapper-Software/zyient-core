/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.core.persistence;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.utils.Timer;
import io.zyient.core.persistence.model.BaseEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class AbstractDataStore<T> implements Closeable {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class Q {
        private String where;
        private Map<String, Boolean> sort;
        private String generatedQuery;
        private final Map<String, Object> parameters = new HashMap<>();

        public boolean hasParameters() {
            return !parameters.isEmpty();
        }

        public Q add(@NonNull String name, @NonNull Object value) {
            parameters.put(name, value);
            return this;
        }

        public Q addAll(@NonNull Map<String, Object> parameters) {
            this.parameters.putAll(parameters);
            return this;
        }

        public Q addSort(@NonNull String name, @NonNull Boolean asc) {
            if (sort == null) {
                sort = new HashMap<>();
            }
            sort.put(name, asc);
            return this;
        }
    }

    public static final String KEY_ENGINE = "DataStore";
    public static final String CONTEXT_KEY_CHECK_UPDATES = "checkUpdates";

    private final DataStoreState state = new DataStoreState();
    protected DataStoreMetrics metrics;
    private AbstractConnection<T> connection = null;
    private DataStoreManager dataStoreManager;
    protected AbstractDataStoreSettings settings;
    protected BaseEnv<?> env;
    private final Map<String, QueryParser<?, ?>> parsers = new HashMap<>();

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    protected void setupMonitoring(@NonNull BaseEnv<?> env) {
        metrics = new DataStoreMetrics(KEY_ENGINE, settings.getName(), getClass().getSimpleName(), env, getClass());
    }

    protected void checkState() throws DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
    }

    public boolean isThreadSafe() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void configure(@NonNull DataStoreManager dataStoreManager,
                          @NonNull AbstractDataStoreSettings settings,
                          @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            this.dataStoreManager = dataStoreManager;
            this.settings = settings;
            this.env = env;
            connection = (AbstractConnection<T>)
                    dataStoreManager().getConnection(settings.getConnectionName(), settings.getConnectionType());
            if (connection == null) {
                throw new Exception(
                        String.format("DataStore connection not found. [name=%s][type=%s]",
                                settings.getConnectionName(), settings.getConnectionType().getCanonicalName()));
            }
            configure();
            setupMonitoring(env);
            state.setState(DataStoreState.EDataStoreState.Available);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    protected <K extends IKey, E extends IEntity<K>> QueryParser<K, E> getParser(@NonNull Class<? extends E> entityType,
                                                                                 @NonNull Class<? extends K> keyTpe) throws Exception {
        if (!parsers.containsKey(entityType.getCanonicalName())) {
            QueryParser<K, E> parser = createParser(entityType, keyTpe);
            parsers.put(entityType.getCanonicalName(), parser);
        }
        return (QueryParser<K, E>) parsers.get(entityType.getCanonicalName());
    }

    protected abstract <K extends IKey, E extends IEntity<K>> QueryParser<K, E> createParser(@NonNull Class<? extends E> entityType,
                                                                                             @NonNull Class<? extends K> keyTpe) throws Exception;


    public abstract void configure() throws ConfigurationException;

    public <E extends IEntity<?>> E create(@NonNull E entity,
                                           @NonNull Class<? extends E> type,
                                           Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            metrics.createCounter().increment();
            try (Timer t = new Timer(metrics.createTimer())) {
                if (entity instanceof BaseEntity<?>) {
                    ((BaseEntity<?>) entity).setCreatedTime(System.nanoTime());
                    ((BaseEntity<?>) entity).setUpdatedTime(System.nanoTime());
                }
                return createEntity(entity, type, context);
            }
        } catch (Throwable t) {
            metrics.createCounterError().increment();
            throw new DataStoreException(t);
        }
    }

    public abstract <E extends IEntity<?>> E createEntity(@NonNull E entity, @NonNull Class<? extends E> type, Context context) throws
            DataStoreException;

    public <E extends IEntity<?>> E update(@NonNull E entity, @NonNull Class<? extends E> type, Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            metrics.updateCounter().increment();
            try (Timer t = new Timer(metrics.updateTimer())) {
                if (entity instanceof BaseEntity<?>) {
                    ((BaseEntity<?>) entity).setUpdatedTime(System.nanoTime());
                }
                return updateEntity(entity, type, context);
            }
        } catch (Throwable t) {
            metrics.updateCounterError().increment();
            throw new DataStoreException(t);
        }
    }

    public abstract <E extends IEntity<?>> E updateEntity(@NonNull E entity,
                                                          @NonNull Class<? extends E> type,
                                                          Context context) throws
            DataStoreException;

    public <E extends IEntity<?>> E upsert(@NonNull E entity, @NonNull Class<? extends E> type, Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            metrics.updateCounter().increment();
            try (Timer t = new Timer(metrics.updateTimer())) {
                if (entity instanceof BaseEntity<?>) {
                    ((BaseEntity<?>) entity).setUpdatedTime(System.nanoTime());
                }
                return upsertEntity(entity, type, context);
            }
        } catch (Throwable t) {
            metrics.updateCounterError().increment();
            throw new DataStoreException(t);
        }
    }

    public abstract <E extends IEntity<?>> E upsertEntity(@NonNull E entity,
                                                          @NonNull Class<? extends E> type,
                                                          Context context) throws
            DataStoreException;

    public <E extends IEntity<?>> boolean delete(@NonNull Object key,
                                                 @NonNull Class<? extends E> type,
                                                 Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            metrics.deleteCounter().increment();
            try (Timer t = new Timer(metrics.deleteTimer())) {
                return deleteEntity(key, type, context);
            }
        } catch (Throwable t) {
            metrics.deleteCounterError().increment();
            throw new DataStoreException(t);
        }
    }

    public abstract <E extends IEntity<?>> boolean deleteEntity(@NonNull Object key,
                                                                @NonNull Class<? extends E> type,
                                                                Context context) throws
            DataStoreException;

    public <E extends IEntity<?>> E find(@NonNull Object key,
                                         @NonNull Class<? extends E> type,
                                         Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        boolean readOnly = isReadOnly(context);
        try {
            metrics.readCounter().increment();
            try (Timer t = new Timer(metrics.readTimer())) {
                return findEntity(key, type, readOnly, context);
            }
        } catch (Throwable t) {
            metrics.readCounterError().increment();
            throw new DataStoreException(t);
        }
    }

    public abstract <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                                        @NonNull Class<? extends E> type,
                                                        boolean readOnly,
                                                        Context context) throws
            DataStoreException;

    public <K extends IKey, E extends IEntity<K>> Cursor<K, E> search(@NonNull Q query,
                                                                      int maxResults,
                                                                      @NonNull Class<? extends K> keyType,
                                                                      @NonNull Class<? extends E> type,
                                                                      Context context) throws
            DataStoreException {
        return search(query, 0, maxResults, keyType, type, context);
    }

    public <K extends IKey, E extends IEntity<K>> Cursor<K, E> search(@NonNull Q query,
                                                                      int currentPage,
                                                                      int maxResults,
                                                                      @NonNull Class<? extends K> keyType,
                                                                      @NonNull Class<? extends E> type,
                                                                      Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            if (maxResults <= 0) {
                maxResults = settings.getMaxResults();
            }
            if (currentPage < 0) {
                currentPage = 0;
            }
            boolean readOnly = isReadOnly(context);
            metrics.searchCounter().increment();
            try (Timer t = new Timer(metrics.searchTimer())) {
                return doSearch(query, currentPage, maxResults, keyType, type, readOnly, context);
            }
        } catch (Throwable t) {
            metrics.searchCounterError().increment();
            throw new DataStoreException(t);
        }
    }


    public abstract <K extends IKey, E extends IEntity<K>> Cursor<K, E> doSearch(@NonNull Q query,
                                                                                 int currentPage,
                                                                                 int maxResults,
                                                                                 @NonNull Class<? extends K> keyType,
                                                                                 @NonNull Class<? extends E> type,
                                                                                 boolean readOnly,
                                                                                 Context context) throws
            DataStoreException;

    public <K extends IKey, E extends IEntity<K>> Cursor<K, E> search(@NonNull Q query,
                                                                      @NonNull Class<? extends K> keyType,
                                                                      @NonNull Class<? extends E> type,
                                                                      Context context) throws
            DataStoreException {
        return search(query, settings.getMaxResults(), keyType, type, context);
    }


    @Override
    public void close() throws IOException {
        try {
            if (dataStoreManager != null) {
                dataStoreManager.close(this);
            }
            if (state.isAvailable())
                state.setState(DataStoreState.EDataStoreState.Closed);
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    public long nextSequence(@NonNull String name) throws Exception {
        state.check(DataStoreState.EDataStoreState.Available);
        return dataStoreManager().nextSequence(name(), name);
    }

    protected boolean checkEntityVersion(Context context) {
        if (context != null && context.containsKey(CONTEXT_KEY_CHECK_UPDATES)) {
            return (boolean) context.get(CONTEXT_KEY_CHECK_UPDATES);
        }
        return true;
    }


    public static Context defaultContext(@NonNull Class<? extends Context> type) throws Exception {
        Context ctx = type.getDeclaredConstructor().newInstance();
        ctx.put(CONTEXT_KEY_CHECK_UPDATES, true);
        return ctx;
    }

    public static final String CONTEXT_KEY_REFRESH = "entity.cache.ignore";
    public static final String CONTEXT_KEY_READ_ONLY = "db.mode.readOnly";

    protected boolean doRefresh(Context context) {
        if (context != null) {
            if (context.containsKey(CONTEXT_KEY_REFRESH)) {
                return (boolean) context.get(CONTEXT_KEY_REFRESH);
            }
        }
        return false;
    }

    protected boolean isReadOnly(Context context) {
        if (context != null) {
            if (context.containsKey(CONTEXT_KEY_READ_ONLY)) {
                return (boolean) context.get(CONTEXT_KEY_READ_ONLY);
            }
        }
        return false;
    }

    public static Context withRefresh(Context context) {
        if (context == null) {
            context = new Context();
        }
        context.put(CONTEXT_KEY_REFRESH, true);
        return context;
    }

    public static Context withReadOnly(Context context, boolean readOnly) {
        if (context == null) {
            context = new Context();
        }
        context.put(CONTEXT_KEY_READ_ONLY, readOnly);
        return context;
    }
}
