/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.stores;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.auditing.AbstractAuditLogger;
import io.zyient.base.core.stores.impl.DataStoreAuditContext;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.utils.Timer;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class AbstractDataStore<T> implements Closeable {
    public static final String KEY_ENGINE = "Intake";
    private final DataStoreState state = new DataStoreState();
    protected DataStoreMetrics metrics;
    private AbstractConnection<T> connection = null;
    private AbstractAuditLogger<?> auditLogger;
    private DataStoreManager dataStoreManager;
    protected AbstractDataStoreSettings settings;
    protected BaseEnv<?> env;

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
        Preconditions.checkNotNull(connection);
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

    public abstract void configure() throws ConfigurationException;

    public <E extends IEntity<?>> E create(@NonNull E entity,
                                           @NonNull Class<? extends E> type,
                                           Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            metrics.createCounter().increment();
            try (Timer t = new Timer(metrics.createTimer())) {
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

    public <E extends IEntity<?>> E find(@NonNull Object key, @NonNull Class<? extends E> type, Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            metrics.readCounter().increment();
            try (Timer t = new Timer(metrics.readTimer())) {
                return findEntity(key, type, context);
            }
        } catch (Throwable t) {
            metrics.readCounterError().increment();
            throw new DataStoreException(t);
        }
    }

    public abstract <E extends IEntity<?>> E findEntity(@NonNull Object key,
                                                        @NonNull Class<? extends E> type,
                                                        Context context) throws
            DataStoreException;

    public <E extends IEntity<?>> BaseSearchResult<E> search(@NonNull String query,
                                                             int offset,
                                                             int maxResults,
                                                             @NonNull Class<? extends E> type,
                                                             Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            metrics.searchCounter().increment();
            try (Timer t = new Timer(metrics.searchTimer())) {
                return doSearch(query, offset, maxResults, type, context);
            }
        } catch (Throwable t) {
            metrics.searchCounterError().increment();
            throw new DataStoreException(t);
        }
    }

    public abstract <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                                        int offset,
                                                                        int maxResults,
                                                                        @NonNull Class<? extends E> type, Context context) throws
            DataStoreException;

    public <E extends IEntity<?>> BaseSearchResult<E> search(@NonNull String query,
                                                             int offset,
                                                             int maxResults,
                                                             Map<String, Object> parameters,
                                                             @NonNull Class<? extends E> type, Context context) throws
            DataStoreException {
        state.check(DataStoreState.EDataStoreState.Available);
        try {
            metrics.searchCounter().increment();
            try (Timer t = new Timer(metrics.searchTimer())) {
                return doSearch(query, offset, maxResults, parameters, type, context);
            }
        } catch (Throwable t) {
            metrics.searchCounterError().increment();
            throw new DataStoreException(t);
        }
    }

    public abstract <E extends IEntity<?>> BaseSearchResult<E> doSearch(@NonNull String query,
                                                                        int offset,
                                                                        int maxResults,
                                                                        Map<String, Object> parameters,
                                                                        @NonNull Class<? extends E> type, Context context) throws
            DataStoreException;

    public <E extends IEntity<?>> BaseSearchResult<E> search(@NonNull String query, @NonNull Class<? extends E> type, Context context) throws
            DataStoreException {
        return search(query, 0, settings.getMaxResults(), type, context);
    }

    public <E extends IEntity<?>> BaseSearchResult<E> search(@NonNull String query,
                                                             Map<String, Object> parameters,
                                                             @NonNull Class<? extends E> type, Context context) throws
            DataStoreException {
        return search(query, 0, settings.getMaxResults(), parameters, type, context);
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

    public abstract DataStoreAuditContext context();
}
