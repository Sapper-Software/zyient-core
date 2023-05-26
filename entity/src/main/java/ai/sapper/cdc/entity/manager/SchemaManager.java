package ai.sapper.cdc.entity.manager;

import ai.sapper.cdc.common.cache.LRUCache;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.InvalidDataError;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.entity.schema.*;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
@Accessors(fluent = true)
public abstract class SchemaManager implements Closeable {
    private final ProcessorState state = new ProcessorState();
    private final Class<? extends SchemaManagerSettings> settingsType;
    private final Map<String, CacheElement<Domain>> domainCache = new HashMap<>();
    private final Map<String, CacheElement<SchemaEntity>> entityCache = new HashMap<>();
    private LRUCache<String, CacheElement<EntitySchema>> schemaCache;

    private SchemaManagerSettings settings;
    private BaseEnv<?> env;
    private SchemaDataHandler handler;
    private DistributedLock schemaLock;

    public SchemaManager(@NonNull Class<? extends SchemaManagerSettings> settingsType) {
        this.settingsType = settingsType;
    }

    public SchemaManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, settingsType);
            reader.read();
            this.settings = (SchemaManagerSettings) reader.settings();

            SchemaDataHandlerSettings hs = readHandlerSettings(xmlConfig, settings);
            settings.setHandlerSettings(hs);

            return init((SchemaManagerSettings) reader.settings(), env);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    public SchemaManager init(@NonNull SchemaManagerSettings settings,
                              @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkNotNull(settings.getHandlerSettings());
        try {
            this.env = env;
            handler = settings.getHandler()
                    .getDeclaredConstructor()
                    .newInstance()
                    .init(settings.getHandlerSettings(), env);
            init(settings);
            this.settings = settings;
            schemaCache = new LRUCache<>(settings.getSchemaCacheSize());
            schemaLock = env.createLock(String.format("SCHEMA_MANAGER-%s", settings.getName()));
            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    private SchemaDataHandlerSettings readHandlerSettings(HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                          SchemaManagerSettings settings) throws Exception {
        ConfigReader reader = new ConfigReader(xmlConfig,
                SchemaDataHandlerSettings.__CONFIG_PATH,
                settings.getHandlerSettingsClass());
        reader.read();
        return (SchemaDataHandlerSettings) reader.settings();
    }

    public Domain getDomain(@NonNull String name) throws Exception {
        Preconditions.checkState(state.isRunning());
        CacheElement<Domain> e = domainCache.get(name);
        if (e == null || e.expired(settings.getCacheTimeout())) {
            synchronized (this) {
                Domain d = handler.fetchDomain(name);
                if (d == null) {
                    throw new Exception(String.format("Domain not found. [name=%s]", name));
                }
                e = new CacheElement<>(d);
                domainCache.put(d.getName(), e);
            }
        }
        return e.element;
    }

    protected Domain createDomain(@NonNull String name,
                                  @NonNull Class<? extends Domain> type) throws Exception {
        Preconditions.checkState(state.isRunning());
        schemaLock.lock();
        try {
            Domain d = getDomain(name);
            if (d != null) {
                if (!d.getClass().equals(type)) {
                    throw new Exception(
                            String.format("Domain exists: type mismatch. [type=%s]", d.getClass().getCanonicalName()));
                }
            } else {
                d = type.getDeclaredConstructor().newInstance();
                d.setName(name);
                d = handler.saveDomain(d);
            }
            CacheElement<Domain> e = new CacheElement<>(d);
            domainCache.put(d.getName(), e);
            return d;
        } finally {
            schemaLock.unlock();
        }
    }

    public Domain updateDomain(@NonNull Domain domain) throws Exception {
        Preconditions.checkState(state.isRunning());
        schemaLock.lock();
        try {
            Domain d = getDomain(domain.getName());
            if (d == null) {
                throw new Exception(String.format("Domain not found. [name=%s]", domain.getName()));
            } else if (d.getUpdatedTime() > domain.getUpdatedTime()) {
                throw new StaleDataError(String.format("Domain instance is stale. [nam%s]", domain.getName()));
            }
            domain = handler.saveDomain(domain);
            CacheElement<Domain> e = new CacheElement<>(domain);
            domainCache.put(domain.getName(), e);
            return domain;
        } finally {
            schemaLock.unlock();
        }
    }

    public boolean deleteDomain(@NonNull String name) throws Exception {
        Preconditions.checkState(state.isRunning());
        Domain d = getDomain(name);
        if (d != null) {
            schemaLock.lock();
            try {
                domainCache.remove(name);
                return handler.deleteDomain(d);
            } finally {
                schemaLock.unlock();
            }
        }
        return false;
    }

    public List<Domain> listDomains() throws Exception {
        Preconditions.checkState(state.isRunning());
        List<Domain> domains = handler.listDomains();
        if (domains != null && !domains.isEmpty()) {
            synchronized (this) {
                for (Domain d : domains) {
                    CacheElement<Domain> e = new CacheElement<>(d);
                    domainCache.put(d.getName(), e);
                }
            }
        }
        return domains;
    }

    public SchemaEntity getEntity(@NonNull String domain,
                                  @NonNull String name) throws Exception {
        Preconditions.checkState(state.isRunning());
        String key = SchemaEntity.key(domain, name);
        CacheElement<SchemaEntity> e = entityCache.get(key);
        if (e == null || e.expired(settings.getCacheTimeout())) {
            synchronized (this) {
                SchemaEntity entity = handler.fetchEntity(domain, name);
                if (entity == null) {
                    throw new Exception(String.format("Entity not found. [domain=%s][entity=%s]", domain, name));
                }
                e = new CacheElement<>(entity);
                entityCache.put(key, e);
            }
        }
        return e.element;
    }

    protected SchemaEntity createEntity(@NonNull String domain,
                                        @NonNull String name,
                                        @NonNull Class<? extends SchemaEntity> type) throws Exception {
        Preconditions.checkState(state.isRunning());
        SchemaEntity entity = getEntity(domain, name);
        if (entity != null) {
            throw new Exception(
                    String.format("Entity already exists: [domain=%s][entity=%s][type=%s]",
                            entity.getDomain(), entity.getEntity(), entity.getClass().getCanonicalName()));
        }
        schemaLock.lock();
        try {
            entity = type.getDeclaredConstructor().newInstance();
            entity.setDomain(domain);
            entity.setEntity(name);
            return handler.saveEntity(entity);
        } finally {
            schemaLock.unlock();
        }
    }

    public SchemaEntity updateEntity(@NonNull SchemaEntity entity) throws Exception {
        Preconditions.checkState(state.isRunning());
        schemaLock.lock();
        try {
            SchemaEntity current = getEntity(entity.getDomain(), entity.getEntity());
            if (current == null) {
                throw new Exception(
                        String.format("Entity not found: [domain=%s][entity=%s]",
                                entity.getDomain(), entity.getEntity()));
            } else if (entity.getUpdatedTime() > current.getUpdatedTime()) {
                throw new StaleDataError(String.format("Entity instance is stale: [domain=%s][entity=%s]",
                        entity.getDomain(), entity.getEntity()));
            }
            return handler.saveEntity(entity);
        } finally {
            schemaLock.unlock();
        }
    }

    public boolean deleteEntity(@NonNull String domain,
                                @NonNull String name) throws Exception {
        Preconditions.checkState(state.isRunning());
        SchemaEntity entity = getEntity(domain, name);
        if (entity != null) {
            schemaLock.lock();
            try {
                String key = SchemaEntity.key(domain, name);
                entityCache.remove(key);
                return handler.deleteEntity(entity);
            } finally {
                schemaLock.unlock();
            }
        }
        return false;
    }

    public EntitySchema getSchema(@NonNull SchemaEntity entity) throws Exception {
        return getSchema(entity, null);
    }

    public EntitySchema getSchema(@NonNull SchemaEntity entity,
                                  SchemaVersion version) throws Exception {
        Preconditions.checkState(state.isRunning());
        EntitySchema schema = null;
        String key = schemaCacheKey(entity);
        Optional<CacheElement<EntitySchema>> op = schemaCache.get(key);
        if (op.isPresent()) {
            CacheElement<EntitySchema> e = op.get();
            if (!e.expired(settings.getCacheTimeout())) {
                schema = e.element;
            }
        }
        if (schema == null) {
            schema = handler.fetchSchema(entity, version);
            if (schema == null) {
                throw new InvalidDataError(EntitySchema.class,
                        String.format("Schema not found. [entity=%s]", entity.toString()));
            }
            CacheElement<EntitySchema> e = new CacheElement<>(schema);
            schemaCache.put(key, e);
        }
        return schema;
    }

    public EntitySchema getSchema(@NonNull String uri) throws Exception {
        Preconditions.checkState(state.isRunning());
        return handler.fetchSchema(uri);
    }

    protected EntitySchema createSchema(@NonNull SchemaEntity entity,
                                        @NonNull Class<? extends EntitySchema> type) throws Exception {
        Preconditions.checkState(state.isRunning());
        EntitySchema schema = getSchema(entity);
        if (schema != null) {
            throw new InvalidDataError(EntitySchema.class,
                    String.format("Schema already exists. [entity=%s]", entity.toString()));
        }
        schemaLock.lock();
        try {
            schema = type.getDeclaredConstructor().newInstance();
            schema.setSchemaEntity(entity);
            schema.setVersion(new SchemaVersion());

            schema = handler.saveSchema(schema);
            synchronized (this) {
                CacheElement<EntitySchema> e = new CacheElement<>(schema);
                String key = schemaCacheKey(entity, schema.getVersion());
                schemaCache.put(key, e);
            }
            return schema;
        } finally {
            schemaLock.unlock();
        }
    }

    public EntitySchema updateSchema(@NonNull EntitySchema schema) throws Exception {
        Preconditions.checkState(state.isRunning());
        schemaLock.lock();
        try {
            EntitySchema current = getSchema(schema.getSchemaEntity());
            if (current == null) {
                throw new Exception(
                        String.format("Schema not found. [entity=%s]",
                                schema.getSchemaEntity().toString()));
            }
            if (current.getUpdatedTime() > schema.getUpdatedTime()) {
                throw new StaleDataError(String.format("Schema instance is stale. [entity=%s]",
                        schema.getSchemaEntity().toString()));
            }
            schema = handler.saveSchema(schema);
            synchronized (this) {
                CacheElement<EntitySchema> e = new CacheElement<>(schema);
                String key = schemaCacheKey(schema.getSchemaEntity());
                schemaCache.put(key, e);
            }
            return schema;
        } finally {
            schemaLock.unlock();
        }
    }

    public boolean deleteSchema(@NonNull SchemaEntity entity,
                                @NonNull SchemaVersion version) throws Exception {
        Preconditions.checkState(state.isRunning());
        schemaLock.lock();
        try {
            String key = schemaCacheKey(entity, version);
            schemaCache.remove(key);
            return handler.deleteSchema(entity, version);
        } finally {
            schemaLock.unlock();
        }
    }

    public abstract Domain createDomain(@NonNull String name) throws Exception;

    public abstract SchemaEntity createEntity(@NonNull String domain,
                                              @NonNull String name) throws Exception;

    public abstract EntitySchema createSchema(@NonNull SchemaEntity entity) throws Exception;

    @Override
    public void close() throws IOException {
        if (state.isRunning() || state.isInitialized())
            state.setState(ProcessorState.EProcessorState.Stopped);
        if (schemaLock != null) {
            schemaLock.close();
        }
        if (handler != null) {
            handler.close();
            handler = null;
        }
        domainCache.clear();
        entityCache.clear();
        if (schemaCache != null) {
            schemaCache.clear();
            schemaCache = null;
        }
    }

    private String schemaCacheKey(SchemaEntity entity) {
        return String.format("%s::%s", entity.getDomain(), entity.getEntity());
    }

    private String schemaCacheKey(SchemaEntity entity, SchemaVersion version) {
        return String.format("%s::%s::%d.%d",
                entity.getDomain(), entity.getEntity(),
                version.getMajorVersion(), version.getMinorVersion());
    }

    protected abstract void init(@NonNull SchemaManagerSettings settings) throws ConfigurationException;

    public static class CacheElement<T> {
        private final T element;
        private final long timestamp;

        public CacheElement(@NonNull T element) {
            this.element = element;
            timestamp = System.currentTimeMillis();
        }

        public boolean expired(long timeout) {
            long delta = System.currentTimeMillis() - timestamp;
            return (delta > timeout);
        }
    }
}
