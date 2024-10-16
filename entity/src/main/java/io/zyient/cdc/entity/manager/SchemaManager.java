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

package io.zyient.cdc.entity.manager;

import com.google.common.base.Preconditions;
import io.zyient.base.common.cache.Expireable;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.InvalidDataError;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.cdc.entity.schema.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class SchemaManager implements Closeable {
    private final ProcessorState state = new ProcessorState();
    private final Class<? extends SchemaManagerSettings> settingsType;
    private final Map<String, Expireable<Domain>> domainCache = new HashMap<>();
    private final Map<String, Expireable<SchemaEntity>> entityCache = new HashMap<>();

    private SchemaManagerSettings settings;
    private BaseEnv<?> env;
    private SchemaDataHandler handler;
    private DistributedLock schemaLock;
    private SchemaCache schemaCache;

    public SchemaManager(@NonNull Class<? extends SchemaManagerSettings> settingsType) {
        this.settingsType = settingsType;
    }

    public SchemaManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull String path,
                              @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, path, settingsType);
            reader.read();
            this.settings = (SchemaManagerSettings) reader.settings();

            SchemaDataHandlerSettings hs = readHandlerSettings(reader.config(), settings);
            settings.setHandlerSettings(hs);

            return init((SchemaManagerSettings) reader.settings(), env);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    public SchemaManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull BaseEnv<?> env) throws ConfigurationException {
        return init(xmlConfig, SchemaManagerSettings.__CONFIG_PATH, env);
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
            schemaCache = new SchemaCache(settings.getSchemaCacheSize(),
                    settings.getCacheTimeout());
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
        Preconditions.checkState(state.isAvailable());
        Expireable<Domain> e = domainCache.get(name);
        if (e == null || e.expired(settings.getCacheTimeout())) {
            synchronized (this) {
                Domain d = handler.fetchDomain(name);
                if (d == null) {
                    return null;
                }
                e = new Expireable<>(d);
                domainCache.put(d.getName(), e);
            }
        }
        return e.element();
    }

    protected Domain createDomain(@NonNull String name,
                                  @NonNull Class<? extends Domain> type) throws Exception {
        Preconditions.checkState(state.isAvailable());
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
            Expireable<Domain> e = new Expireable<>(d);
            domainCache.put(d.getName(), e);
            return d;
        } finally {
            schemaLock.unlock();
        }
    }

    public Domain updateDomain(@NonNull Domain domain) throws Exception {
        Preconditions.checkState(state.isAvailable());
        schemaLock.lock();
        try {
            Domain d = getDomain(domain.getName());
            if (d == null) {
                throw new Exception(String.format("Domain not found. [name=%s]", domain.getName()));
            } else if (d.getUpdatedTime() > domain.getUpdatedTime()) {
                throw new StaleDataError(String.format("Domain instance is stale. [nam%s]", domain.getName()));
            }
            domain = handler.saveDomain(domain);
            Expireable<Domain> e = new Expireable<>(domain);
            domainCache.put(domain.getName(), e);
            return domain;
        } finally {
            schemaLock.unlock();
        }
    }

    public boolean deleteDomain(@NonNull String name) throws Exception {
        Preconditions.checkState(state.isAvailable());
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
        Preconditions.checkState(state.isAvailable());
        List<Domain> domains = handler.listDomains();
        if (domains != null && !domains.isEmpty()) {
            synchronized (this) {
                for (Domain d : domains) {
                    Expireable<Domain> e = new Expireable<>(d);
                    domainCache.put(d.getName(), e);
                }
            }
        }
        return domains;
    }

    public SchemaEntity getEntity(@NonNull String domain,
                                  @NonNull String name) throws Exception {
        Preconditions.checkState(state.isAvailable());
        String key = SchemaEntity.key(domain, name);
        Expireable<SchemaEntity> e = entityCache.get(key);
        if (e == null || e.expired(settings.getCacheTimeout())) {
            synchronized (this) {
                SchemaEntity entity = handler.fetchEntity(domain, name);
                if (entity == null) {
                    return null;
                }
                e = new Expireable<>(entity);
                entityCache.put(key, e);
            }
        }
        return e.element();
    }

    protected SchemaEntity createEntity(@NonNull String domain,
                                        @NonNull String name,
                                        @NonNull Class<? extends SchemaEntity> type) throws Exception {
        Preconditions.checkState(state.isAvailable());
        SchemaEntity entity = getEntity(domain, name);
        if (entity != null) {
            throw new Exception(
                    String.format("Entity already exists: [domain=%s][entity=%s][type=%s]",
                            entity.getDomain(), entity.getEntity(), entity.getClass().getCanonicalName()));
        }
        schemaLock.lock();
        try {
            Domain d = getDomain(domain);
            if (d == null) {
                throw new Exception(String.format("Domain not found. [domain=%s]", domain));
            }
            entity = type.getDeclaredConstructor().newInstance();
            entity.setDomain(domain);
            entity.setEntity(name);
            entity.setOptions(d.getDefaultOptions());
            return handler.saveEntity(entity);
        } finally {
            schemaLock.unlock();
        }
    }

    public SchemaEntity updateEntity(@NonNull SchemaEntity entity) throws Exception {
        Preconditions.checkState(state.isAvailable());
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
        Preconditions.checkState(state.isAvailable());
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

    public <T extends EntitySchema> T getSchema(@NonNull SchemaEntity entity,
                                                @NonNull Class<? extends T> type) throws Exception {
        return getSchema(entity, (SchemaVersion) null, type);
    }

    @SuppressWarnings("unchecked")
    public <T extends EntitySchema> T getSchema(@NonNull SchemaEntity entity,
                                                SchemaVersion version,
                                                @NonNull Class<? extends T> type) throws Exception {
        Preconditions.checkState(state.isAvailable());
        EntitySchema schema = schemaCache.get(entity, version);
        if (schema == null) {
            schema = handler.fetchSchema(entity, version);
            if (schema == null) {
                return null;
            }
            if (schema.getSchema() == null) {
                Schema avs = new Schema.Parser().parse(schema.getSchemaStr());
                schema.setSchema(avs);
            }
            schemaCache.addSchema(entity, schema);
        }
        if (!ReflectionHelper.isSuperType(type, schema.getClass())) {
            throw new InvalidDataError(EntitySchema.class,
                    String.format("Schema Type mismatch: [expected=%s][actual=%s]",
                            type.getCanonicalName(), schema.getClass().getCanonicalName()));
        }
        return (T) schema;
    }

    @SuppressWarnings("unchecked")
    public <T extends EntitySchema> T getSchema(@NonNull SchemaEntity entity,
                                                @NonNull String uri,
                                                @NonNull Class<? extends T> type) throws Exception {
        Preconditions.checkState(state.isAvailable());
        EntitySchema schema = handler.fetchSchema(entity, uri);
        if (schema == null) {
            return null;
        }
        if (schema.getSchema() == null) {
            Schema avs = new Schema.Parser().parse(schema.getSchemaStr());
            schema.setSchema(avs);
        }
        if (!ReflectionHelper.isSuperType(type, schema.getClass())) {
            throw new InvalidDataError(EntitySchema.class,
                    String.format("Schema Type mismatch: [expected=%s][actual=%s]",
                            type.getCanonicalName(), schema.getClass().getCanonicalName()));
        }
        return (T) schema;
    }

    @SuppressWarnings("unchecked")
    protected <T extends EntitySchema> T createSchema(@NonNull SchemaEntity entity,
                                                      @NonNull Class<? extends T> type) throws Exception {
        Preconditions.checkState(state.isAvailable());
        schemaLock.lock();
        try {
            EntitySchema schema = getSchema(entity, type);
            if (schema != null) {
                throw new InvalidDataError(EntitySchema.class,
                        String.format("Schema already exists. [entity=%s]", entity.toString()));
            }

            schema = type.getDeclaredConstructor().newInstance();
            schema.setSchemaEntity(entity);
            schema.setVersion(new SchemaVersion());

            schema = handler.saveSchema(schema);
            schemaCache.addSchema(entity, schema);
            return (T) schema;
        } finally {
            schemaLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends EntitySchema> T createSchema(@NonNull T schema,
                                                      @NonNull SchemaEntity entity) throws Exception {
        Preconditions.checkState(state.isAvailable());
        schemaLock.lock();
        try {
            EntitySchema current = getSchema(entity, schema.getVersion(), schema.getClass());
            if (current != null) {
                throw new InvalidDataError(EntitySchema.class,
                        String.format("Schema already exists. [entity=%s]", entity.toString()));
            }

            schema = (T) handler.saveSchema(schema);
            schemaCache.addSchema(entity, schema);
            return (T) schema;
        } finally {
            schemaLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends EntitySchema> T updateSchema(@NonNull T schema) throws Exception {
        Preconditions.checkState(state.isAvailable());
        schemaLock.lock();
        try {
            EntitySchema current = getSchema(schema.getSchemaEntity(), schema.getVersion(), schema.getClass());
            if (current != null) {
                if (current.getUpdatedTime() > schema.getUpdatedTime()) {
                    throw new StaleDataError(String.format("Schema instance is stale. [entity=%s]",
                            schema.getSchemaEntity().toString()));
                }
            }

            schema = (T) handler.saveSchema(schema);
            schemaCache.addSchema(schema.getSchemaEntity(), schema);
            return schema;
        } finally {
            schemaLock.unlock();
        }
    }

    public String getSchemaEntityURI(@NonNull SchemaEntity entity) throws Exception {
        return getSchemaEntityURI(entity, null);
    }

    public String getSchemaEntityURI(@NonNull SchemaEntity entity, SchemaVersion version) throws Exception {
        Preconditions.checkState(state.isAvailable());
        return handler.getSchemaEntityURI(entity, version);
    }

    public boolean deleteSchema(@NonNull SchemaEntity entity,
                                @NonNull SchemaVersion version) throws Exception {
        Preconditions.checkState(state.isAvailable());
        schemaLock.lock();
        try {
            schemaCache.removeSchema(entity, version);
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
        if (state.isAvailable() || state.isInitialized())
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

    protected abstract void init(@NonNull SchemaManagerSettings settings) throws ConfigurationException;
}
