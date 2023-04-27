package ai.sapper.cdc.entity;

import ai.sapper.cdc.common.cache.LRUCache;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.common.schema.StaleDataError;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.entity.schema.EntityDiff;
import ai.sapper.cdc.entity.schema.EntitySchema;
import ai.sapper.cdc.entity.schema.SchemaManager;
import ai.sapper.cdc.entity.schema.SchemaMapping;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;

@Getter
@Accessors(fluent = true)
public class EntitySchemaManager extends SchemaManager {
    @Getter
    @Setter
    public static class EntityCacheEntry {
        private CDCSchemaEntity entity;
        private SchemaVersion version;
        private long updateTimestamp;
    }

    @Getter
    @Setter
    public static class SchemaCacheEntry {
        private EntitySchema entity;
        private long updateTimestamp;
    }

    @Getter
    @Setter
    public static class MappingCacheEntry {
        private SchemaMapping mapping;
        private long updateTimestamp;
    }

    public static class EntityCache {
        public static final long ENTITY_CACHE_TIMEOUT = 1000 * 60 * 15;
        private final LRUCache<String, EntityCacheEntry> entityCache = new LRUCache<>(1024);
        private final LRUCache<String, SchemaCacheEntry> schemaCache = new LRUCache<>(1024);
        private final LRUCache<String, MappingCacheEntry> mappingCache = new LRUCache<>(1024);

        public SchemaMapping getMapping(@NonNull String path) {
            if (mappingCache.containsKey(path)) {
                MappingCacheEntry me = mappingCache.get(path).get();
                if (System.currentTimeMillis() - me.updateTimestamp < ENTITY_CACHE_TIMEOUT) {
                    return me.mapping;
                } else {
                    mappingCache.remove(path);
                }
            }
            return null;
        }

        public SchemaMapping update(@NonNull String path,
                                    @NonNull SchemaMapping mapping) {
            MappingCacheEntry me = null;
            if (mappingCache.containsKey(path)) {
                me = mappingCache.get(path).get();
            } else {
                me = new MappingCacheEntry();
            }
            me.mapping = mapping;
            me.updateTimestamp = System.currentTimeMillis();
            mappingCache.put(path, me);
            return mapping;
        }

        public EntitySchema getSchema(@NonNull String path) {
            if (schemaCache.containsKey(path)) {
                SchemaCacheEntry se = schemaCache.get(path).get();
                if (System.currentTimeMillis() - se.updateTimestamp < ENTITY_CACHE_TIMEOUT) {
                    return se.entity;
                } else {
                    schemaCache.remove(path);
                }
            }
            return null;
        }

        public EntitySchema update(@NonNull String path,
                                   @NonNull EntitySchema schema) {
            SchemaCacheEntry se = null;
            if (schemaCache.containsKey(path)) {
                se = schemaCache.get(path).get();
            } else {
                se = new SchemaCacheEntry();
            }
            se.entity = schema;
            se.updateTimestamp = System.currentTimeMillis();
            schemaCache.put(path, se);

            return schema;
        }

        public boolean deleteSchema(@NonNull String path) {
            if (schemaCache.containsKey(path)) {
                return schemaCache.remove(path);
            }
            return false;
        }

        public CDCSchemaEntity get(@NonNull String path) {
            if (entityCache.containsKey(path)) {
                EntityCacheEntry ce = entityCache.get(path).get();
                if (System.currentTimeMillis() - ce.updateTimestamp < ENTITY_CACHE_TIMEOUT) {
                    return ce.entity;
                } else {
                    entityCache.remove(path);
                }
            }
            return null;
        }

        public SchemaVersion getVersion(@NonNull String path) {
            if (entityCache.containsKey(path)) {
                EntityCacheEntry ce = entityCache.get(path).get();
                if (System.currentTimeMillis() - ce.updateTimestamp < ENTITY_CACHE_TIMEOUT) {
                    return ce.version;
                } else {
                    entityCache.remove(path);
                }
            }
            return null;
        }

        public CDCSchemaEntity update(@NonNull String path, @NonNull CDCSchemaEntity entity) {
            EntityCacheEntry ce = null;
            if (entityCache.containsKey(path)) {
                ce = entityCache.get(path).get();
            } else {
                ce = new EntityCacheEntry();
            }
            ce.entity = entity;
            ce.updateTimestamp = System.currentTimeMillis();
            entityCache.put(path, ce);

            return entity;
        }

        public CDCSchemaEntity update(@NonNull String path,
                                      @NonNull CDCSchemaEntity entity,
                                      @NonNull SchemaVersion version) {
            EntityCacheEntry ce = null;
            if (entityCache.containsKey(path)) {
                ce = entityCache.get(path).get();
            } else {
                ce = new EntityCacheEntry();
            }
            ce.entity = entity;
            ce.version = version;
            ce.updateTimestamp = System.currentTimeMillis();
            entityCache.put(path, ce);

            return entity;
        }

        public boolean delete(@NonNull String path) {
            if (entityCache.containsKey(path)) {
                return entityCache.remove(path);
            }
            return false;
        }
    }

    public static final String PATH_MAPPING = "mapping";
    public static final String PATH_SCHEMA = "schema";
    public static final String PATH_VERSION = "version";

    private String sourceSchemaPath;
    private final EntityCache entityCache = new EntityCache();

    /**
     * @param xmlConfig
     * @param manger
     * @return
     * @throws ConfigurationException
     */
    @Override
    public SchemaManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull ConnectionManager manger,
                              @NonNull String environment,
                              @NonNull String source,
                              @NonNull String lockPath) throws ConfigurationException {
        super.init(xmlConfig,
                manger,
                environment,
                source,
                lockPath);
        return this;
    }

    public CDCSchemaEntity save(@NonNull CDCSchemaEntity schemaEntity) throws Exception {
        writeLock().lock();
        try {
            String path = getEntityBasePath(schemaEntity);
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                CDCSchemaEntity current = getEntity(schemaEntity);
                if (current != null) {
                    if (current.getUpdatedTime() > schemaEntity.getUpdatedTime()) {
                        throw new StaleDataError(
                                String.format("Schema Entity instance is stale. [entity=%s]",
                                        schemaEntity.toString()));
                    }
                }
            } else {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
            schemaEntity.setUpdatedTime(System.currentTimeMillis());
            schemaEntity.setZkPath(path);
            String json = JSONUtils.asString(schemaEntity, CDCSchemaEntity.class);
            client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
            return entityCache.update(schemaEntity.getZkPath(), schemaEntity);
        } finally {
            writeLock().unlock();
        }
    }

    public CDCSchemaEntity getEntity(@NonNull String domain, @NonNull String entity) throws Exception {
        return getEntity(new SchemaEntity(domain, entity));
    }

    public CDCSchemaEntity getEntity(@NonNull SchemaEntity schemaEntity) throws Exception {
        String path = getEntityBasePath(schemaEntity);
        return getEntity(path);
    }

    private CDCSchemaEntity getEntity(String path) throws Exception {
        CDCSchemaEntity entity = entityCache.get(path);
        if (entity == null) {
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    entity = JSONUtils.read(data, CDCSchemaEntity.class);
                    return entityCache.update(entity.getZkPath(), entity);
                }
            }
        }
        return entity;
    }

    public EntitySchema getSourceSchema(@NonNull SchemaEntity schemaEntity,
                                        @NonNull SchemaVersion version) throws Exception {
        return get(schemaEntity, version);
    }

    public <T extends SchemaMapping> T createMapping(@NonNull CDCSchemaEntity schemaEntity,
                                                     @NonNull Class<T> type) throws Exception {
        return createMapping(schemaEntity, currentVersion(schemaEntity), type);
    }

    public <T extends SchemaMapping> T createMapping(@NonNull CDCSchemaEntity schemaEntity,
                                                     @NonNull SchemaVersion version,
                                                     @NonNull Class<T> type) throws Exception {
        EntitySchema schema = getEntitySchema(schemaEntity, version);
        if (schema == null) {
            throw new Exception(
                    String.format("No schema found for entity. [entity=%s][version=%s]",
                            schemaEntity.toString(), version.toString()));
        }
        return createMapping(schema, schemaEntity, version, type);
    }

    @SuppressWarnings("unchecked")
    public <T extends SchemaMapping> T createMapping(@NonNull EntitySchema schema,
                                                     @NonNull SchemaEntity schemaEntity,
                                                     @NonNull SchemaVersion version,
                                                     @NonNull Class<T> type) throws Exception {
        SchemaMapping mapping = type.getDeclaredConstructor().newInstance();
        mapping.setEntitySchema(schema);
        mapping.setTargetEntity(new SchemaEntity(schemaEntity.getDomain(), schemaEntity.getEntity()));
        return (T) saveMapping(schemaEntity, version, mapping);
    }

    public SchemaMapping saveMapping(@NonNull CDCSchemaEntity schemaEntity,
                                     @NonNull SchemaMapping mapping) throws Exception {
        return saveMapping(schemaEntity, currentVersion(schemaEntity), mapping);
    }

    public SchemaMapping saveMapping(@NonNull SchemaEntity schemaEntity,
                                     @NonNull SchemaVersion version,
                                     @NonNull SchemaMapping mapping) throws Exception {
        String path = getMappingsPath(schemaEntity, version);
        CuratorFramework client = zkConnection().client();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        if (mapping.getEntitySchema() != null) {
            mapping.setEntitySchemaPath(mapping.getEntitySchema().getZkPath());
        }
        mapping.validate();

        String json = JSONUtils.asString(mapping, SchemaMapping.class);
        client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
        return entityCache.update(path, mapping);
    }

    public SchemaMapping getMapping(@NonNull CDCSchemaEntity schemaEntity) throws Exception {
        return getMapping(schemaEntity, currentVersion(schemaEntity));
    }

    public SchemaMapping getMapping(@NonNull CDCSchemaEntity schemaEntity,
                                    @NonNull SchemaVersion version) throws Exception {
        String path = getMappingsPath(schemaEntity, version);
        return getMapping(path);
    }

    public SchemaMapping getMapping(@NonNull String path) throws Exception {
        SchemaMapping mapping = entityCache().getMapping(path);
        if (mapping == null) {
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    mapping = JSONUtils.read(data, SchemaMapping.class);
                    mapping.read(this);
                    return entityCache.update(path, mapping);
                }
            }
        }
        return mapping;
    }

    private String getMappingsPath(@NonNull SchemaEntity schemaEntity,
                                   @NonNull SchemaVersion version) {
        return new PathUtils.ZkPathBuilder(getZkPath())
                .withPath(schemaEntity.getDomain())
                .withPath(schemaEntity.getEntity())
                .withPath(PATH_MAPPING)
                .withPath(version.path())
                .build();
    }

    public EntitySchema create(@NonNull EntitySchema entitySchema) throws Exception {
        writeLock().lock();
        try {
            CDCSchemaEntity se = getEntity(entitySchema.getSchemaEntity());
            if (se == null) {
                throw new Exception(String.format("Entity definition not found. [entity=%s]",
                        entitySchema.getSchemaEntity().toString()));
            }
            EntitySchema current = getEntitySchema(se);
            if (current != null) {
                throw new StaleDataError(
                        String.format("Entity Schema already exists. [entity=%s]",
                                entitySchema.getSchemaEntity()));
            }
            SchemaVersion version = null;
            version = new SchemaVersion();

            entitySchema.setVersion(version);
            return save(entitySchema, true);
        } finally {
            writeLock().unlock();
        }
    }

    private SchemaVersion checkVersion(EntitySchema schema, EntitySchema current) throws Exception {
        EntityDiff diff = schema.diff(current);
        if (diff != null) {
            SchemaVersion version = new SchemaVersion();
            if (diff.hasDroppedFields() || diff.hasNewFields()) {
                version.setMajorVersion(current.getVersion().getMajorVersion() + 1);
                version.setMinorVersion(0);
            } else {
                version.setMajorVersion(current.getVersion().getMajorVersion());
                version.setMinorVersion(current.getVersion().getMinorVersion() + 1);
            }
            schema.setVersion(version);
        }
        return schema.getVersion();
    }

    public EntitySchema update(@NonNull EntitySchema entitySchema) throws Exception {
        writeLock().lock();
        try {
            CDCSchemaEntity se = getEntity(entitySchema.getSchemaEntity());
            if (se == null) {
                throw new Exception(String.format("Entity definition not found. [entity=%s]",
                        entitySchema.getSchemaEntity().toString()));
            }
            EntitySchema current = getEntitySchema(se);
            if (current == null) {
                throw new Exception(String.format("No existing schema found. [entity=%s]",
                        entitySchema.getSchemaEntity().toString()));
            }
            if (entitySchema.getVersion() != null) {
                if (!entitySchema.getVersion().equals(current.getVersion())) {
                    throw new StaleDataError(
                            String.format("Entity Schema instance is stale: Read version is older. [entity=%s]",
                                    entitySchema.getSchemaEntity().toString()));
                }
            } else {
                entitySchema.setVersion(current.getVersion());
            }
            checkVersion(entitySchema, current);
            boolean saveVersion = false;
            if (current.getVersion().equals(entitySchema.getVersion())) {
                if (current.getUpdatedTime() > entitySchema.getUpdatedTime()) {
                    throw new StaleDataError(
                            String.format("Entity Schema instance is stale. [entity=%s]",
                                    entitySchema.getSchemaEntity().toString()));
                }
            } else if (current.getVersion().compare(entitySchema.getVersion()) > 0) {
                saveVersion = true;
            }
            return save(entitySchema, saveVersion);
        } finally {
            writeLock().unlock();
        }
    }

    private EntitySchema save(@NonNull EntitySchema entitySchema,
                              boolean saveVersion) throws Exception {
        if (entitySchema.getVersion() == null) {
            entitySchema.setVersion(new SchemaVersion());
        }
        String path = getEntitySchemaPath(entitySchema.getSchemaEntity(), entitySchema.getVersion());
        CuratorFramework client = zkConnection().client();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }

        if (saveVersion) {
            String vpath = getEntitySchemaVersionPath(entitySchema.getSchemaEntity());
            if (client.checkExists().forPath(vpath) == null) {
                client.create().creatingParentsIfNeeded().forPath(vpath);
            }
            String json = JSONUtils.asString(entitySchema.getVersion(), SchemaVersion.class);
            client.setData().forPath(vpath, json.getBytes(StandardCharsets.UTF_8));
        }
        path = getEntitySchemaPath(entitySchema.getSchemaEntity(), entitySchema.getVersion());
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        entitySchema.setUpdatedTime(System.currentTimeMillis());
        if (entitySchema.getSchema() != null) {
            entitySchema.setSchemaStr(entitySchema.getSchema().toString(false));
        }
        entitySchema.setZkPath(path);
        CDCSchemaEntity se = getEntity(entitySchema.getSchemaEntity());
        entitySchema.setEntityZkPath(se.getZkPath());

        String json = JSONUtils.asString(entitySchema, entitySchema.getClass());
        client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

        entitySchema = entityCache.update(path, entitySchema);
        String sep = getEntityBasePath(entitySchema.getSchemaEntity());
        CDCSchemaEntity cse = getEntity(sep);
        entityCache.update(sep, cse, entitySchema.getVersion());

        return entitySchema;
    }

    public boolean delete(@NonNull SchemaEntity schemaEntity) throws Exception {
        writeLock().lock();
        try {
            String path = getEntityBasePath(schemaEntity);
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                client.delete().forPath(path);
                return true;
            }
            return false;
        } finally {
            writeLock().unlock();
        }
    }

    public boolean delete(@NonNull SchemaEntity schemaEntity,
                          @NonNull SchemaVersion version) throws Exception {
        writeLock().lock();
        try {
            String path = getEntitySchemaPath(schemaEntity, version);
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                client.delete().forPath(path);
                entityCache.deleteSchema(path);
                return true;
            }
            return false;
        } finally {
            writeLock().unlock();
        }
    }

    public EntitySchema getEntitySchema(@NonNull CDCSchemaEntity schemaEntity) throws Exception {
        return getEntitySchema(schemaEntity, currentVersion(schemaEntity));
    }

    public EntitySchema getEntitySchema(@NonNull CDCSchemaEntity schemaEntity,
                                        @NonNull SchemaVersion version) throws Exception {
        String path = getEntitySchemaPath(schemaEntity, version);
        return getEntitySchema(path);
    }

    public EntitySchema getEntitySchema(@NonNull String path) throws Exception {
        EntitySchema schema = entityCache.getSchema(path);
        if (schema == null) {
            CuratorFramework client = zkConnection().client();
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    schema = JSONUtils.read(data, EntitySchema.class);
                    if (schema != null) {
                        CDCSchemaEntity se = getEntity(schema.getEntityZkPath());
                        if (se == null) {
                            throw new Exception(String.format("Entity not found. [path=%s]", schema.getEntityZkPath()));
                        }
                        schema.setSchemaEntity(se);
                        return entityCache.update(path, schema);
                    }
                }
            }
        }
        return schema;
    }

    private SchemaVersion currentVersion(CDCSchemaEntity schemaEntity) throws Exception {
        String sep = getEntityBasePath(schemaEntity);
        SchemaVersion version = entityCache.getVersion(sep);
        if (version == null) {
            CuratorFramework client = zkConnection().client();
            version = new SchemaVersion();
            String path = getEntitySchemaVersionPath(schemaEntity);
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    version = JSONUtils.read(data, SchemaVersion.class);
                    entityCache.update(sep, schemaEntity, version);
                }
            }
        }
        return version;
    }


    public EntitySchema getSourceSchema(@NonNull SchemaEntity schemaEntity) throws Exception {
        return get(schemaEntity);
    }

    public String getEntityBasePath(@NonNull SchemaEntity schemaEntity) {
        return new PathUtils.ZkPathBuilder(getZkPath())
                .withPath(schemaEntity.getDomain())
                .withPath(schemaEntity.getEntity())
                .build();
    }

    public String getEntitySchemaBasePath(@NonNull SchemaEntity schemaEntity) {
        return new PathUtils.ZkPathBuilder(getZkPath())
                .withPath(schemaEntity.getDomain())
                .withPath(schemaEntity.getEntity())
                .withPath(PATH_SCHEMA)
                .build();
    }

    public String getEntitySchemaDomainPath(@NonNull String domain) {
        return new PathUtils.ZkPathBuilder(getZkPath())
                .withPath(domain)
                .build();
    }

    public String getEntitySchemaVersionPath(@NonNull SchemaEntity schemaEntity) {
        return new PathUtils.ZkPathBuilder(getEntitySchemaBasePath(schemaEntity))
                .withPath(PATH_VERSION)
                .build();
    }

    public String getEntitySchemaPath(@NonNull SchemaEntity schemaEntity,
                                      @NonNull SchemaVersion version) {
        return new PathUtils.ZkPathBuilder(getEntitySchemaBasePath(schemaEntity))
                .withPath(version.path())
                .build();
    }
}
