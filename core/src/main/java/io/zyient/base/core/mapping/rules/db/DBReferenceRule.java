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

package io.zyient.base.core.mapping.rules.db;

import com.google.common.base.Preconditions;
import io.zyient.base.common.errors.Errors;
import io.zyient.base.common.model.PropertyModel;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.MappingExecutor;
import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.mapping.rules.*;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.impl.rdbms.RdbmsDataStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class DBReferenceRule<T, K extends IKey, E extends IEntity<K>> extends ExternalRule<T> {
    private RdbmsDataStore dataStore;
    private String query;
    private Map<String, PropertyModel> whereFields = null;
    private Class<? extends K> keyType;
    private Class<? extends E> refEntityType;
    private DBRuleHandler<T, K, E> handler;
    private Map<String, PropertyModel> sourceFields = null;
    private Map<String, PropertyModel> targetMappings = null;

    @Override
    protected Object doEvaluate(@NonNull MappedResponse<T> data) throws RuleValidationError, RuleEvaluationError {
        Preconditions.checkNotNull(dataStore);
        try {
            AbstractDataStore.Q q = new AbstractDataStore.Q()
                    .where(query);
            if (whereFields != null && !whereFields.isEmpty()) {
                Map<String, Object> params = new HashMap<>();
                for (String key : whereFields.keySet()) {
                    PropertyModel field = whereFields.get(key);
                    Object value = MappingReflectionHelper.getProperty(field, data);
                    params.put(key, value);
                }
                q.addAll(params);
            }
            Cursor<K, E> cursor = dataStore.search(q, keyType, refEntityType, null);
            return process(data, cursor);
        } catch (RuleValidationError | RuleEvaluationError e) {
            throw e;
        } catch (RuntimeException re) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    targetFieldString(),
                    errorCode(),
                    Errors.getDefault().get(__RULE_TYPE, errorCode()).getMessage(),
                    re);
        } catch (Throwable t) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    targetFieldString(),
                    errorCode(),
                    Errors.getDefault().get(__RULE_TYPE, errorCode()).getMessage(),
                    t);
        }
    }

    private Object process(@NonNull MappedResponse<T> response,
                           @NonNull Cursor<K, E> cursor) throws RuleValidationError, RuleEvaluationError {
        if (handler != null) {
            return handler.handle(response, cursor);
        }
        try {
            List<E> entities = cursor.nextPage();
            if (entities != null && !entities.isEmpty()) {
                E entity = entities.get(0);
                if (targetMappings != null && !targetMappings.isEmpty()) {
                    for (String key : targetMappings.keySet()) {
                        PropertyModel target = targetMappings.get(key);
                        PropertyModel source = sourceFields.get(key);
                        Preconditions.checkNotNull(source);
                        Object value = source.getter().invoke(entity);
                        if (value != null) {
                            MappingReflectionHelper.setProperty(target, response, value);
                        }
                    }
                }
                return entity;
            }
            return null;
        } catch (Throwable t) {
            throw new RuleEvaluationError(name(),
                    entityType(),
                    targetFieldString(),
                    errorCode(),
                    Errors.getDefault().get(__RULE_TYPE, errorCode()).getMessage(),
                    t);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void setup(@NonNull RuleConfig config) throws ConfigurationException {
        Preconditions.checkArgument(config instanceof DBRuleConfig);
        try {
            dataStore = MappingExecutor.defaultInstance()
                    .dataStoreManager()
                    .getDataStore(((DBRuleConfig) config).getDataStore(), RdbmsDataStore.class);
            if (dataStore == null) {
                throw new ConfigurationException(String.format("Data Store not found. [name=%s]",
                        ((DBRuleConfig) config).getDataStore()));
            }
            query = config.getRule();
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
                    whereFields.put(param, field);
                    index++;
                }
            }
            keyType = (Class<? extends K>) ((DBRuleConfig) config).getKeyType();
            refEntityType = (Class<? extends E>) ((DBRuleConfig) config).getEntityType();
            if (((DBRuleConfig) config).getFieldMappings() != null) {
                sourceFields = new HashMap<>();
                targetMappings = new HashMap<>();
                for (String key : ((DBRuleConfig) config).getFieldMappings().keySet()) {
                    PropertyModel source = ReflectionUtils.findProperty(refEntityType, key);
                    if (source == null) {
                        throw new Exception(String.format("[source] Property not found. [entity=%s][field=%s]",
                                refEntityType.getCanonicalName(), key));
                    }
                    String tf = ((DBRuleConfig) config).getFieldMappings().get(key);
                    PropertyModel target = MappingReflectionHelper.findField(tf, entityType());
                    if (target == null) {
                        throw new Exception(String.format("[target] Property not found. [entity=%s][field=%s]",
                                entityType().getCanonicalName(), tf));
                    }
                    sourceFields.put(key, source);
                    targetMappings.put(key, target);
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
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }
}
