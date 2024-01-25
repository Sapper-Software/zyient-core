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

package io.zyient.core.persistence.impl.rdbms;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.QueryParser;
import jakarta.persistence.*;
import lombok.NonNull;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.lang.reflect.Field;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

public class SqlQueryParser<K extends IKey, E extends IEntity<K>> extends QueryParser<K, E> {
    public SqlQueryParser(@NonNull Class<? extends K> keyType,
                          @NonNull Class<? extends E> entityType) throws Exception {
        super(keyType, entityType);
    }

    @Override
    protected String buildSort(@NonNull Map<String, Boolean> sort) {
        StringBuilder builder = new StringBuilder(" ORDER BY ");
        boolean first = true;
        for (String name : sort.keySet()) {
            if (first) first = false;
            else {
                builder.append(", ");
            }
            String dir = "ASC";
            if (!sort.get(name)) {
                dir = "DESC";
            }
            builder.append(name)
                    .append(" ")
                    .append(dir);
        }
        return builder.toString();
    }

    @Override
    protected void process(AbstractDataStore.@NonNull Q query,
                           @NonNull Select select) throws Exception {
        PlainSelect ps = select.getSelectBody(PlainSelect.class);
        String hql = String.format("FROM %s WHERE %s",
                ps.getFromItem().toString(), ps.getWhere().toString());
        if (ps.getOrderByElements() != null) {
            StringBuilder builder = new StringBuilder(hql)
                    .append(" ORDER BY ");
            boolean first = true;
            for (OrderByElement oe : ps.getOrderByElements()) {
                if (first) {
                    first = false;
                } else {
                    builder.append(", ");
                }
                builder.append(oe.toString());
            }
            hql = builder.toString();
        }
        query.generatedQuery(hql);
    }

    @Override
    protected String extractEntity(@NonNull Class<? extends E> type) {
        String entity = type.getSimpleName();
        if (type.isAnnotationPresent(Entity.class)) {
            Entity e = type.getAnnotation(Entity.class);
            if (!Strings.isNullOrEmpty(e.name())) {
                entity = e.name();
            }
        }
        return entity;
    }

    @Override
    protected Map<String, QueryField> extractFields(@NonNull Class<? extends E> type) throws Exception {
        Field[] fields = ReflectionHelper.getAllFields(type);
        Preconditions.checkNotNull(fields);
        Map<String, QueryField> map = new HashMap<>();
        for (Field field : fields) {
            if (ignore(field)) continue;
            if (processField(field)) {
                QueryField qf = new QueryField()
                        .path(field.getName())
                        .field(field)
                        .alias(column(field));
                map.put(qf.path(), qf);
            }
        }
        return map;
    }

    @Override
    protected Map<String, QueryField> extractKeyFields(@NonNull Class<? extends E> entityType,
                                                       @NonNull Class<? extends K> type) throws Exception {
        Field[] fields = ReflectionHelper.getAllFields(entityType);
        Preconditions.checkNotNull(fields);
        Class<?> idType = null;
        Field idField = null;
        for (Field field : fields) {
            if (field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(EmbeddedId.class)) {
                idField = field;
                idType = field.getType();
                break;
            }
        }
        if (idField == null) {
            throw new Exception(String.format("Primary Key not defined for type. [type=%s]",
                    entityType.getCanonicalName()));
        }
        if (!ReflectionHelper.isSuperType(idType, type)) {
            throw new Exception(String.format("Invalid Key type specified. [expected=%s][key type=%s]",
                    idType.getCanonicalName(), type.getCanonicalName()));
        }
        if (processField(idField) || isNativeKey(idField)) {
            QueryField qf = new QueryField()
                    .path(idField.getName())
                    .alias(column(idField))
                    .field(idField);
            return Map.of(qf.path(), qf);
        } else {
            if (!type.isAnnotationPresent(Embeddable.class)) {
                throw new Exception(String.format("Key type is not embeddable. [type=%s]", type.getCanonicalName()));
            }
            Field[] kfields = ReflectionHelper.getAllFields(idType);
            Preconditions.checkNotNull(kfields);
            Map<String, QueryField> map = new HashMap<>();
            String prefix = idField.getName();
            for (Field kf : kfields) {
                if (ignore(kf)) continue;
                if (!processField(kf)) {
                    throw new Exception(String.format("Embedded type not supported. [type=%s]",
                            kf.getType().getCanonicalName()));
                }
                QueryField qf = new QueryField()
                        .path(String.format("%s.%s", prefix, kf.getName()))
                        .alias(column(kf))
                        .field(kf);
                map.put(qf.path(), qf);
            }
            return map;
        }
    }

    private boolean processField(Field field) {
        return ReflectionHelper.isPrimitiveTypeOrString(field) ||
                field.getType().isEnum() ||
                field.getType().equals(Date.class);
    }

    private boolean ignore(Field field) {
        if (field.isAnnotationPresent(Transient.class)) {
            return true;
        }
        return false;
    }

    private String column(Field field) {
        String name = field.getName();
        if (field.isAnnotationPresent(Column.class)) {
            Column c = field.getAnnotation(Column.class);
            if (!Strings.isNullOrEmpty(c.name())) {
                name = c.name();
            }
        }
        return name;
    }
}
