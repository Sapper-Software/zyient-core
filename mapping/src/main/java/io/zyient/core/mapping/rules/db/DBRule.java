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

package io.zyient.core.mapping.rules.db;

import com.google.common.base.Preconditions;
import io.zyient.base.common.cache.LRUCache;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.ChecksumUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.common.utils.beans.PropertyDef;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.errors.Errors;
import io.zyient.core.mapping.model.RuleDef;
import io.zyient.core.mapping.rules.*;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.env.DataStoreEnv;
import io.zyient.core.persistence.impl.rdbms.RdbmsDataStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.*;

@Getter
@Accessors(fluent = true)
public abstract class DBRule<T, K extends IKey, E extends IEntity<K>> extends ExternalRule<T> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    protected static class FieldProperty {
        private PropertyDef property;
        private String field;

        public FieldProperty(PropertyDef property, String field) {
            this.property = property;
            this.field = field;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    protected static class CacheRecord<K extends IKey, E extends IEntity<K>> {
        private String query;
        private Map<String, Object> where;
        private List<E> result;
        private long timestamp;
    }

    protected RdbmsDataStore dataStore;
    protected String query;
    protected Map<String, FieldProperty> whereFields = null;
    protected Class<? extends K> keyType;
    protected Class<? extends E> refEntityType;
    protected DBRuleHandler<T, K, E> handler;
    protected Map<String, FieldProperty> sourceFields = null;
    protected Map<String, FieldProperty> targetMappings = null;
    protected LRUCache<String, CacheRecord<K, E>> cache;
    protected DBRuleConfig config;

    @Override
    @SuppressWarnings("unchecked")
    public void setup(@NonNull RuleConfig cfg) throws ConfigurationException {
        Preconditions.checkArgument(cfg instanceof DBRuleConfig);
        Preconditions.checkArgument(env() instanceof DataStoreEnv<?>);
        config = (DBRuleConfig) cfg;
        try {
            dataStore = ((DataStoreEnv<?>) env()).dataStoreManager()
                    .getDataStore(((DBRuleConfig) config).getDataStore(), RdbmsDataStore.class);
            if (dataStore == null) {
                throw new ConfigurationException(String.format("Data Store not found. [name=%s]",
                        ((DBRuleConfig) config).getDataStore()));
            }
            query = config.getExpression();
            Map<String, String> fs = MappingReflectionHelper.extractFields(query);
            if (fs != null && !fs.isEmpty()) {
                whereFields = new HashMap<>();
                int index = 0;
                for (String key : fs.keySet()) {
                    String f = fs.get(key);
                    PropertyDef field = MappingReflectionHelper.findField(f, entityType());
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
            keyType = (Class<? extends K>) ((DBRuleConfig) config).getKeyType();
            refEntityType = (Class<? extends E>) ((DBRuleConfig) config).getEntityType();
            if (((DBRuleConfig) config).getFieldMappings() != null) {
                sourceFields = new HashMap<>();
                targetMappings = new HashMap<>();
                for (String key : ((DBRuleConfig) config).getFieldMappings().keySet()) {
                    PropertyDef source = ReflectionHelper.findProperty(refEntityType, key);
                    if (source == null) {
                        throw new Exception(String.format("[source] Property not found. [entity=%s][field=%s]",
                                refEntityType.getCanonicalName(), key));
                    }
                    String tf = ((DBRuleConfig) config).getFieldMappings().get(key);
                    PropertyDef target = MappingReflectionHelper.findField(tf, entityType());
                    if (target == null) {
                        throw new Exception(String.format("[target] Property not found. [entity=%s][field=%s]",
                                entityType().getCanonicalName(), tf));
                    }
                    String fieldKey = MappingReflectionHelper.normalizeField(key);
                    sourceFields.put(fieldKey, new FieldProperty(source, key));
                    String targetField = MappingReflectionHelper.normalizeField(config.getFieldMappings().get(key));
                    targetMappings.put(fieldKey, new FieldProperty(target, targetField));
                }
            }
            if (getRuleType() == RuleType.Transformation) {
                if (targetMappings == null || targetMappings.isEmpty()) {
                    throw new ConfigurationException(String
                            .format("Target mappings required for transformation. [rule=%s]", name()));
                }
            }
            if (((DBRuleConfig) config).getHandler() != null) {
                handler = (DBRuleHandler<T, K, E>) ((DBRuleConfig) config).getHandler()
                        .getDeclaredConstructor()
                        .newInstance();
                handler.configure((DBRuleConfig) config, this);
            }
            if (config.isUseCache()) {
                cache = new LRUCache<>(config.getCacheSize());
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Rule<T> configure(@NonNull RuleDef def,
                             @NonNull BaseEnv<?> env) throws ConfigurationException {
        DBRuleConfig config = (DBRuleConfig) new DBRuleConfig()
                .from(def);
        return configure(config, env);
    }

    @Override
    public Rule<T> withContentDir(@NonNull File contentDir) {
        return this;
    }

    @Override
    protected Object doEvaluate(@NonNull T data) throws RuleValidationError, RuleEvaluationError {
        Preconditions.checkNotNull(dataStore);
        try {
            List<E> entities = fetch(data);
            if (handler != null) {
                return handler.handle(data, entities);
            }
            return process(data, entities);
        } catch (RuleValidationError | RuleEvaluationError e) {
            throw e;
        } catch (RuntimeException re) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    expression(),
                    errorCode(),
                    Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode()).getMessage(),
                    re);
        } catch (Throwable t) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    expression(),
                    errorCode(),
                    Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode()).getMessage(),
                    t);
        }
    }

    private List<E> fetch(T data) throws Exception {
        AbstractDataStore.Q q = new AbstractDataStore.Q()
                .where(query);
        if (whereFields != null && !whereFields.isEmpty()) {
            Map<String, Object> params = new HashMap<>();
            for (String key : whereFields.keySet()) {
                FieldProperty field = whereFields.get(key);
                Object value = MappingReflectionHelper.getProperty(field.field(), field.property(), data);
                params.put(key, value);
            }
            q.addAll(params);
        }
        String key = key(q);
        if (cache != null) {
            if (cache.containsKey(key)) {
                Optional<CacheRecord<K, E>> o = cache.get(key);
                if (o.isPresent()) {
                    CacheRecord<K, E> record = o.get();
                    if (System.currentTimeMillis() - record.timestamp < config.getCacheTimeout().normalized()) {
                        DefaultLogger.debug(String.format("Found in cache. [where=%s][predicates=%s]",
                                q.where(), q.parameters()));
                        return record.result;
                    }
                }
            }
        }
        try (Cursor<K, E> cursor = dataStore.search(q, keyType, refEntityType, null)) {
            List<E> entities = new ArrayList<>();
            while (true) {
                List<E> r = cursor.nextPage();
                if (r == null || r.isEmpty())
                    break;
                entities.addAll(r);
            }
            CacheRecord<K, E> record = new CacheRecord<>();
            record.query = q.where();
            record.where = q.parameters();
            record.result = entities;
            record.timestamp = System.currentTimeMillis();
            cache.put(key, record);
            return entities;
        }
    }

    private String key(AbstractDataStore.Q query) throws Exception {
        StringBuilder str = new StringBuilder(query.where());
        if (query.parameters() != null && !query.parameters().isEmpty()) {
            String json = JSONUtils.asString(query.parameters());
            str.append(":").append(json);
        }
        return ChecksumUtils.generateHash(str.toString());
    }

    protected abstract Object process(@NonNull T response,
                                      List<E> entities) throws RuleValidationError, RuleEvaluationError;
}
