/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package ai.sapper.cdc.core.stores;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.auditing.AbstractAuditLogger;
import ai.sapper.cdc.core.model.IEntity;
import ai.sapper.cdc.core.stores.impl.DataStoreAuditContext;
import ai.sapper.cdc.core.utils.Timer;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractDataStore<T> implements Closeable {
    public static final String KEY_ENGINE = "Intake";
    public final DataStoreState state = new DataStoreState();
    @Setter(AccessLevel.NONE)
    protected DataStoreMetrics metrics;
    @Setter(AccessLevel.NONE)
    private AbstractConnection<T> connection = null;
    @Setter(AccessLevel.NONE)
    private long threadId;
    @Setter(AccessLevel.NONE)
    private AbstractDataStoreSettings config;
    private AbstractAuditLogger<?> auditLogger;
    @Setter(AccessLevel.NONE)
    private DataStoreManager dataStoreManager;
    private AbstractDataStoreSettings settings;
    private BaseEnv<?> env;

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    public AbstractDataStore() {
        threadId = Thread.currentThread().getId();
    }

    public void setupMonitoring(@NonNull BaseEnv<?> env) {
        metrics = new DataStoreMetrics(KEY_ENGINE, settings.getName(), getClass().getSimpleName(), env, getClass());
    }

    protected void checkThread() throws DataStoreException {
        long threadId = Thread.currentThread().getId();
        if (this.threadId != threadId) {
            throw new DataStoreException(String.format("Thread instance exception. [expected=%d][current=%d]", this.threadId, threadId));
        }
    }

    public AbstractDataStore<T> withConnection(@NonNull AbstractConnection<T> connection) {
        this.connection = connection;
        return this;
    }

    public void configure(@NonNull DataStoreManager dataStoreManager,
                          @NonNull AbstractDataStoreSettings settings,
                          @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            this.dataStoreManager = dataStoreManager;
            this.settings = settings;
            this.env = env;
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

    public abstract DataStoreAuditContext context();
}
