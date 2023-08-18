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

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.model.IEntity;
import ai.sapper.cdc.core.stores.annotations.EGeneratedType;
import ai.sapper.cdc.core.stores.annotations.GeneratedId;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.hibernate.Session;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;

import javax.persistence.EmbeddedId;
import javax.persistence.Id;
import javax.persistence.Query;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

public class IDGenerator {
    public static void process(@NonNull IEntity<?> entity,
                               @NonNull Session session) throws DataStoreException {
        Field[] fields = ReflectionUtils.getAllFields(entity.getClass());
        if (fields != null) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(EmbeddedId.class)) {
                    process(entity, field, session);
                    break;
                }
            }
        }
    }

    public static void process(@NonNull IEntity<?> entity,
                               @NonNull Field field,
                               @NonNull Session session) throws DataStoreException {
        Preconditions.checkArgument(field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(EmbeddedId.class));
        try {
            if (field.isAnnotationPresent(Id.class)) {
                if (field.isAnnotationPresent(GeneratedId.class)) {
                    GeneratedId gi = field.getAnnotation(GeneratedId.class);
                    if (gi.type() == EGeneratedType.UUID) {
                        ReflectionUtils.setObjectValue(entity, field, UUID.randomUUID().toString());
                    } else {
                        Long value = nextSequenceValue(gi.sequence(), session);
                        if (value != null) {
                            ReflectionUtils.setObjectValue(entity, field, value);
                        }
                    }
                }
            } else {
                Class<?> type = field.getType();
                Field[] fields = ReflectionUtils.getAllFields(type);
                if (fields != null && fields.length > 0) {
                    for (Field f : fields) {
                        if (f.isAnnotationPresent(GeneratedId.class)) {
                            Object fv = ReflectionUtils.getFieldValue(entity, field);
                            if (fv == null) {
                                fv = ReflectionUtils.createInstance(field.getType());
                                ReflectionUtils.setObjectValue(entity, field, fv);
                            }
                            GeneratedId gi = f.getAnnotation(GeneratedId.class);
                            if (gi.type() == EGeneratedType.UUID) {
                                ReflectionUtils.setObjectValue(fv, f, UUID.randomUUID().toString());
                            } else {
                                Long value = nextSequenceValue(gi.sequence(), session);
                                if (value != null) {
                                    ReflectionUtils.setObjectValue(fv, f, value);
                                }
                            }
                            break;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private static Long nextSequenceValue(String sequence, Session session) throws DataStoreException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sequence));
        String sql = null;
        Dialect dialect = ((SessionFactoryImplementor) session.getSessionFactory()).getJdbcServices().getDialect();
        if (dialect == null) {
            throw new DataStoreException("Error getting SQL dialect from Session...");
        }
        if (dialect instanceof MySQLDialect) {
            sql = String.format("SELECT NEXT VALUE FOR %s", sequence);
        }
        /*
        else if (dialect instanceof Oracle8iDialect) {
            sql = String.format("SELECT %s.nextval FROM dual", sequence);
        }
         */
        else {
            throw new DataStoreException(
                    String.format("DB Dialect not supported for Generated ID. [dialect=%s]",
                            dialect.getClass().getCanonicalName()));
        }

        Long value = null;
        if (!Strings.isNullOrEmpty(sql)) {
            Query query = (Query) session.createNativeQuery(sql);
            List<?> result = query.getResultList();
            if (result != null && !result.isEmpty()) {
                Object ret = result.get(0);
                if (ret instanceof BigInteger) {
                    value = ((BigInteger) ret).longValue();
                } else if (ret instanceof Long) {
                    value = (Long) ret;
                } else if (ret instanceof Integer) {
                    value = Long.valueOf((Integer) ret);
                }
            }
        }
        if (value == null) {
            throw new DataStoreException(
                    String.format("Error fetching sequence value. [dialect=%s][sequence=%s]",
                            dialect.getClass().getCanonicalName(), sequence));
        }
        DefaultLogger.debug(String.format("Fetched Sequence :[%s=%d]", sequence, value));
        return value;
    }
}
