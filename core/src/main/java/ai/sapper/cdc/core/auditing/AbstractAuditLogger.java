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

package ai.sapper.cdc.core.auditing;

import ai.sapper.cdc.common.audit.AuditRecord;
import ai.sapper.cdc.common.audit.EAuditType;
import ai.sapper.cdc.common.cache.MapThreadCache;
import ai.sapper.cdc.common.config.ConfigPath;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.entity.IKey;
import ai.sapper.cdc.common.model.entity.IKeyed;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.core.stores.AbstractDataStore;
import ai.sapper.cdc.core.stores.DataStoreException;
import ai.sapper.cdc.core.stores.DataStoreManager;
import ai.sapper.cdc.core.stores.TransactionDataStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class to define Audit loggers.
 *
 * @param <C> - Data Store Connection type.
 */
@Getter
@Accessors(fluent = true)
public abstract class AbstractAuditLogger<C> implements Closeable {
    private final ProcessorState state = new ProcessorState();
    private final Class<? extends AuditLoggerSettings> settingsType;
    private DataStoreManager dataStoreManager;
    private IAuditSerDe serializer;
    @Getter(AccessLevel.NONE)
    private MapThreadCache<String, AuditRecord> cache;
    private AuditLoggerSettings settings;

    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    public boolean isDefaultLogger() {
        Preconditions.checkNotNull(settings);
        return settings().isDefaultLogger();
    }

    protected AbstractAuditLogger(@NonNull Class<? extends AuditLoggerSettings> settingsType) {
        this.settingsType = settingsType;
    }

    /**
     * Set the data store to be used by this audit logger.
     *
     * @param dataStoreManager - Data Store handle.
     * @return - Self
     */
    public AbstractAuditLogger<C> withDataStoreManager(@NonNull DataStoreManager dataStoreManager) {
        this.dataStoreManager = dataStoreManager;
        return this;
    }

    /**
     * Set the data serializer for this logger.
     *
     * @param serializer - Entity data serializer.
     * @return - Self
     */
    public AbstractAuditLogger<C> withSerializer(@NonNull IAuditSerDe serializer) {
        this.serializer = serializer;
        return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public AbstractDataStore<C> getDataStore(boolean checkTransaction) throws AuditException, DataStoreException {
        AbstractDataStore<C> dataStore =
                dataStoreManager().getDataStore(settings.getDataStoreName(),
                        (Class<? extends AbstractDataStore<C>>) settings.getDataStoreType());
        if (dataStore == null) {
            throw new AuditException(
                    String.format("Data Store not found. [type=%s][name=%s]",
                            settings.getDataStoreType().getCanonicalName(), settings.getDataStoreName()));
        }
        if (checkTransaction && dataStore.connection().hasTransactionSupport()) {
            TransactionDataStore ts = (TransactionDataStore) dataStore;
            if (!ts.isInTransaction()) {
                ts.beingTransaction();
            }
        }
        return dataStore;
    }

    /**
     * Configure this type instance.
     *
     * @param xmlConfig - Handle to the configuration node.
     * @throws ConfigurationException
     */
    public AbstractAuditLogger<C> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                       @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkNotNull(dataStoreManager);
        try {
            ConfigPath cp = settingsType.getAnnotation(ConfigPath.class);
            ConfigReader reader = new ConfigReader(xmlConfig, cp.path(), settingsType);
            reader.read();
            settings = (AuditLoggerSettings) reader.settings();
            if (settings.getSerializerClass() != null) {
                IAuditSerDe serializer = settings.getSerializerClass()
                        .getDeclaredConstructor().newInstance();
                withSerializer(serializer);
                DefaultLogger.info(
                        String.format("Using default serializer. [type=%s]", serializer.getClass().getCanonicalName()));
            }
            if (settings.isUseCache()) {
                cache = new MapThreadCache<>();
                DefaultLogger.debug(String.format("Using audit cache. [cache size=%d]", settings.getMaxCacheSize()));
            }
            state.setState(ProcessorState.EProcessorState.Initialized);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }


    /**
     * Write an audit record for the audit type and specified entity record.
     * Will use the default serializer if set, else throw exception.
     *
     * @param type       - Audit record type.
     * @param entity     - Entity to audit
     * @param entityType - Class of the the entity.
     * @param user       - User principal
     * @param <T>        - Entity record type.
     * @return - Created Audit record.
     * @throws AuditException
     */
    public <T extends IKeyed<?>> AuditRecord write(@NonNull Class<?> dataStoreType,
                                                   @NonNull String dataStoreName,
                                                   @NonNull EAuditType type,
                                                   @NonNull T entity,
                                                   @NonNull Class<? extends T> entityType,
                                                   String changeDelta,
                                                   String changeContext,
                                                   @NonNull Principal user) throws AuditException {
        try {
            state.check(ProcessorState.EProcessorState.Running);
            if (serializer == null) {
                throw new AuditException(String.format("[logger=%s] No serializer defined.", getClass().getCanonicalName()));
            }
            return write(dataStoreType,
                    dataStoreName,
                    type,
                    entity,
                    entityType,
                    changeDelta,
                    changeContext,
                    user,
                    serializer);
        } catch (Exception ex) {
            throw new AuditException(ex);
        }
    }

    /**
     * Write an audit record for the audit type and specified entity record.
     * Will use the default serializer if set, else throw exception.
     *
     * @param type       - Audit record type.
     * @param entity     - Entity to audit
     * @param entityType - Class of the the entity.
     * @param user       - User principal
     * @param serializer - Record serializer to use.
     * @param <T>        - Entity record type.
     * @return - Created Audit record.
     * @throws AuditException
     */
    public <T extends IKeyed<?>> AuditRecord write(@NonNull Class<?> dataStoreType,
                                                   @NonNull String dataStoreName,
                                                   @NonNull EAuditType type,
                                                   @NonNull T entity,
                                                   @NonNull Class<? extends T> entityType,
                                                   String changeDelta,
                                                   String changeContext,
                                                   @NonNull Principal user,
                                                   @NonNull IAuditSerDe serializer) throws AuditException {
        Preconditions.checkState(dataStoreManager != null);
        try {
            state.check(ProcessorState.EProcessorState.Running);
            AuditRecord record = createAuditRecord(dataStoreType,
                    dataStoreName,
                    type,
                    entity,
                    entityType,
                    changeDelta,
                    changeContext,
                    user,
                    serializer);
            if (settings.isUseCache()) {
                cache.put(record.getKey().stringKey(), record);
                if (cache.size() > settings.getMaxCacheSize()) {
                    flush();
                }
            } else {
                record = writeToStore(record);
            }
            return record;
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    public <T extends IKeyed<?>> AuditRecord writeToStore(@NonNull AuditRecord record) throws AuditException {
        Preconditions.checkState(dataStoreManager != null);
        try {
            state.check(ProcessorState.EProcessorState.Running);
            AbstractDataStore<C> dataStore = getDataStore(true);
            record = dataStore.createEntity(record, record.getClass(), null);
            return record;
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public void discard() throws AuditException {
        try {
            if (settings.isUseCache()) {
                cache.clear();
                DefaultLogger.debug("Discarded cached audit records...");
            }
            AbstractDataStore<C> dataStore = getDataStore(false);
            if (dataStore == null) {
                throw new AuditException(
                        String.format("Data Store not found. [type=%s][name=%s]",
                                settings.getDataStoreType().getCanonicalName(), settings.getDataStoreName()));
            }
            try {
                if (dataStore.connection().hasTransactionSupport()) {
                    TransactionDataStore ts = (TransactionDataStore) dataStore;
                    if (ts.isInTransaction()) {
                        ts.rollback();
                    }
                }
            } finally {
                dataStore.close();
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    public int flush() throws AuditException {
        try {
            int size = 0;
            if (settings.isUseCache()) {
                Map<String, AuditRecord> records = cache.get();
                if (records != null) {
                    size = records.size();
                    if (size > 0) {
                        for (String key : records.keySet()) {
                            AuditRecord record = records.get(key);
                            writeToStore(record);
                        }
                    }
                    cache.clear();
                    DefaultLogger.debug(String.format("Flushed [%d] audit records to store.", size));
                }
            }
            AbstractDataStore<C> dataStore = getDataStore(false);
            try (dataStore) {
                if (dataStore == null) {
                    throw new AuditException(
                            String.format("Data Store not found. [type=%s][name=%s]",
                                    settings.getDataStoreType().getCanonicalName(), settings.getDataStoreName()));
                }
                if (dataStore.connection().hasTransactionSupport()) {
                    TransactionDataStore ts = (TransactionDataStore) dataStore;
                    if (ts.isInTransaction()) {
                        ts.commit();
                    }
                }
                return size;
            }
        } catch (Throwable t) {
            throw new AuditException(t);
        }
    }

    protected <T extends IKeyed<?>> AuditRecord createAuditRecord(@NonNull Class<?> dataStoreType,
                                                                  @NonNull String dataStoreName,
                                                                  @NonNull EAuditType type,
                                                                  @NonNull T entity,
                                                                  @NonNull Class<? extends T> entityType,
                                                                  String changeDelta,
                                                                  String changeContext,
                                                                  @NonNull Principal user,
                                                                  @NonNull IAuditSerDe serializer) throws AuditException {
        try {
            AuditRecord record = new AuditRecord(dataStoreType, dataStoreName, entityType, user.getName());
            record.setAuditType(type);
            String data = serializer.serialize(entity, entityType);
            record.setEntityData(data);
            record.setEntityId(entity.getKey().stringKey());
            if (!Strings.isNullOrEmpty(changeDelta)) {
                record.setChangeDelta(changeDelta);
            }
            if (!Strings.isNullOrEmpty(changeContext)) {
                record.setChangeContext(changeContext);
            }
            return record;
        } catch (Throwable ex) {
            throw new AuditException(ex);
        }
    }

    /**
     * Extract and fetch the entities from the audit records
     * retrieved by the entity key and entity type.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @param <K>        - Entity Key Type
     * @param <T>        - Entity Type.
     * @return - List of extracted entity records.
     * @throws AuditException
     */
    public <K extends IKey, T extends IKeyed<K>> List<T> fetch(@NonNull K key,
                                                               @NonNull Class<? extends T> entityType) throws AuditException {
        Preconditions.checkNotNull(serializer);
        return fetch(key, entityType, serializer);
    }

    /**
     * Extract and fetch the entities from the audit records
     * retrieved by the entity key and entity type.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @param serializer - Entity data serializer
     * @param <K>        - Entity Key Type
     * @param <T>        - Entity Type.
     * @return - List of extracted entity records.
     * @throws AuditException
     */
    public <K extends IKey, T extends IKeyed<K>> List<T> fetch(@NonNull K key,
                                                               @NonNull Class<? extends T> entityType,
                                                               @NonNull IAuditSerDe serializer) throws AuditException {
        Preconditions.checkState(dataStoreManager != null);
        try {
            state.check(ProcessorState.EProcessorState.Running);
            Collection<AuditRecord> records = find(key, entityType);
            if (records != null && !records.isEmpty()) {
                List<T> entities = new ArrayList<>(records.size());
                for (AuditRecord record : records) {
                    T entity = (T) serializer.deserialize(record.getEntityData(), entityType);
                    DefaultLogger.trace(entity);
                    entities.add(entity);
                }
                return entities;
            }
            return null;
        } catch (Exception ex) {
            throw new AuditException(ex);
        }
    }

    /**
     * Search for entity records based on the query string specified.
     *
     * @param query      - Query String
     * @param entityType - Record Entity type.
     * @param <T>        - Entity Type
     * @return - Collection of fetched records.
     * @throws AuditException
     */
    public <T extends IKeyed<?>> Collection<T> search(@NonNull String query,
                                                      @NonNull Class<? extends T> entityType) throws AuditException {
        Preconditions.checkState(serializer != null);
        return search(query, entityType, serializer);
    }

    /**
     * Search for entity records based on the query string specified.
     *
     * @param query      - Query String
     * @param entityType - Record Entity type.
     * @param serializer - Entity data serializer
     * @param <T>        - Entity Type
     * @return - Collection of fetched records.
     * @throws AuditException
     */
    public abstract <T extends IKeyed<?>> Collection<T> search(@NonNull String query,
                                                               @NonNull Class<? extends T> entityType,
                                                               @NonNull IAuditSerDe serializer) throws AuditException;

    /**
     * Fetch all audit records for the specified entity type and entity key.
     *
     * @param key        - Entity Key
     * @param entityType - Entity Type
     * @param <K>        - Entity Key Type
     * @param <T>        - Entity Type.
     * @return - List of audit records.
     * @throws AuditException
     */
    public abstract <K extends IKey, T extends IKeyed<K>> Collection<AuditRecord> find(@NonNull K key,
                                                                                       @NonNull Class<? extends T> entityType) throws AuditException;
}
