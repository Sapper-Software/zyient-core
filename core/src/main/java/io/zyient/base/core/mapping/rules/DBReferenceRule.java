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

package io.zyient.base.core.mapping.rules;

import com.google.common.base.Preconditions;
import io.zyient.base.common.errors.Errors;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.mapping.MappingExecutor;
import io.zyient.base.core.mapping.model.MappedResponse;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.Cursor;
import io.zyient.base.core.stores.impl.rdbms.RdbmsDataStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class DBReferenceRule<T, K extends IKey, E extends IEntity<K>> extends ExternalRule<T> {
    private RdbmsDataStore dataStore;
    private String query;
    private Map<String, Field> whereFields = null;
    private Class<? extends K> keyType;
    private Class<? extends E> refEntityType;

    @Override
    protected Object doEvaluate(@NonNull MappedResponse<T> data) throws RuleValidationError, RuleEvaluationError {
        Preconditions.checkNotNull(dataStore);
        try {
            AbstractDataStore.Q q = new AbstractDataStore.Q()
                    .where(query);
            if (whereFields != null && !whereFields.isEmpty()) {
                Map<String, Object> params = new HashMap<>();
                for (String key : whereFields.keySet()) {
                    Field field = whereFields.get(key);
                    Object value = ReflectionUtils.getFieldValue(data, field);
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

    protected Object process(@NonNull MappedResponse<T> response,
                             @NonNull Cursor<K, E> cursor) throws RuleEvaluationError, RuleEvaluationError {
        try {
            List<E> entities = cursor.nextPage();
            if (entities != null && !entities.isEmpty()) {
                if (targetFields() != null && !targetFields().isEmpty()) {
                    Map<String, String> mapping = ((DBRuleConfig) config()).getFieldMapping();
                    E entity = entities.get(0);
                    for (String e : mapping.keySet()) {
                        Field ef = ReflectionUtils.findField(refEntityType, e);
                        Preconditions.checkNotNull(ef);
                        Object v = ReflectionUtils.getFieldValue(entity, ef, true);
                        if (v != null) {
                            Field tf = targetFields().get(mapping.get(e));
                            Preconditions.checkNotNull(tf);
                            ReflectionUtils.setValue(v, response.entity(), tf);
                        }
                    }
                }
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
                    Field field = MappingReflectionHelper.findField(f, entityType());
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
            if (targetFields() != null && !targetFields().isEmpty()) {
                Map<String, String> mapping = ((DBRuleConfig) config).getFieldMapping();
                if (mapping == null || mapping.size() != targetFields().size()) {
                    throw new Exception(String.format("Missing/Invalid fetched field mapping. [fields=%s]",
                            targetFieldString()));
                }
                for (String name : targetFields().keySet()) {
                    boolean found = false;
                    for (String target : mapping.values()) {
                        if (target.compareTo(name) == 0) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new Exception(String.format("Fetch mapping not defined for field. [field=%s]", name));
                    }
                }
                for (String name : mapping.keySet()) {
                    Field f = ReflectionUtils.findField(refEntityType, name);
                    if (f == null) {
                        throw new Exception(String.format("Field not found. [entity=%s][field=%s]",
                                refEntityType.getCanonicalName(), name));
                    }
                }
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }
}
