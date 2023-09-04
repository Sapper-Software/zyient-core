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

import io.zyient.cdc.entity.schema.EntitySchema;
import io.zyient.cdc.entity.schema.SchemaEntity;
import io.zyient.cdc.entity.schema.SchemaVersion;
import io.zyient.base.common.cache.Expireable;
import io.zyient.base.common.cache.LRUCache;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SchemaCache {

    private final LRUCache<String, Expireable<EntitySchema>> schemaCache;
    private final Map<String, SchemaVersion> versionCache = new HashMap<>();
    private final long cacheTimeout;

    public SchemaCache(int size,
                       long cacheTimeout) {
        schemaCache = new LRUCache<>(size);
        this.cacheTimeout = cacheTimeout;
    }

    public synchronized SchemaVersion checkAndAdd(@NonNull SchemaEntity entity,
                                                  @NonNull SchemaVersion version) {
        String key = schemaVersionCacheKey(entity);
        if (versionCache.containsKey(key)) {
            SchemaVersion current = versionCache.get(key);
            if (version.compare(current) > 0) {
                versionCache.put(key, version);
                return version;
            } else {
                return current;
            }
        } else {
            versionCache.put(key, version);
            return version;
        }
    }

    public SchemaVersion getLastVersion(@NonNull SchemaEntity entity) {
        String key = schemaVersionCacheKey(entity);
        return versionCache.get(key);
    }

    public synchronized void addSchema(@NonNull SchemaEntity entity,
                                       @NonNull EntitySchema schema) {
        checkAndAdd(entity, schema.getVersion());
        Expireable<EntitySchema> ce = new Expireable<>(schema);
        String key = schemaCacheKey(entity, schema.getVersion());
        schemaCache.put(key, ce);
    }

    public synchronized void removeSchema(@NonNull SchemaEntity entity,
                                          @NonNull SchemaVersion version) {
        String key = schemaCacheKey(entity, version);
        if (schemaCache.containsKey(key)) {
            schemaCache.remove(key);
        }
        key = schemaVersionCacheKey(entity);
        if (versionCache.containsKey(key)) {
            SchemaVersion current = versionCache.get(key);
            if (version.compare(current) == 0) {
                versionCache.remove(key);
            }
        }
    }

    public EntitySchema get(@NonNull SchemaEntity entity,
                            SchemaVersion version) {
        if (version == null) {
            return get(entity);
        }
        String key = schemaCacheKey(entity, version);
        if (schemaCache.containsKey(key)) {
            Optional<Expireable<EntitySchema>> o = schemaCache.get(key);
            if (o.isPresent()) {
                Expireable<EntitySchema> ce = o.get();
                if (!ce.expired(cacheTimeout)) {
                    return ce.element();
                } else {
                    schemaCache.remove(key);
                }
            }
        }
        return null;
    }

    public EntitySchema get(@NonNull SchemaEntity entity) {
        String key = schemaVersionCacheKey(entity);
        if (versionCache.containsKey(key)) {
            SchemaVersion version = versionCache.get(key);
            return get(entity, version);
        }
        return null;
    }

    protected String schemaCacheKey(@NonNull SchemaEntity entity,
                                    @NonNull SchemaVersion version) {
        return String.format("%s::%s::%d.%d",
                entity.getDomain(), entity.getEntity(),
                version.getMajorVersion(), version.getMinorVersion());
    }


    protected String schemaVersionCacheKey(@NonNull SchemaEntity entity) {
        return String.format("%s::%s",
                entity.getDomain(), entity.getEntity());
    }

    public void clear() {
        versionCache.clear();
        schemaCache.clear();
    }
}
