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

package io.zyient.base.core.stores.impl.mongo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Property;
import dev.morphia.annotations.Transient;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.QueryParser;
import lombok.NonNull;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

public class MongoQueryParser<K extends IKey, E extends IEntity<K>> extends QueryParser<K, E> {
    public MongoQueryParser(@NonNull Class<? extends K> keyType,
                            @NonNull Class<? extends E> entityType) throws Exception {
        super(keyType, entityType);
    }

    @Override
    protected void process(AbstractDataStore.@NonNull Q query,
                           @NonNull Select select) throws Exception {
        PlainSelect ps = select.getSelectBody(PlainSelect.class);
        Expression where = ps.getWhere();
        if (where != null) {
            process(where);
        }
        query.generatedQuery(select.toString());
    }

    private void process(Expression expression) throws Exception {
        if (expression instanceof BinaryExpression op) {
            process(op.getLeftExpression());
            process(op.getRightExpression());
        } else if (expression instanceof Column column) {
            String name = column.toString();
            if (!entityFields().containsKey(name)) {
                throw new Exception(String.format("Column not found. [name=%s]", name));
            }
            QueryField qf = entityFields().get(name);
            String[] parts = qf.alias().split("\\.");
            if (parts.length == 1) {
                column.setColumnName(qf.alias());
            } else {
                StringBuilder sb = new StringBuilder();
                String n = null;
                for(int ii=0; ii < parts.length; ii++) {
                    if (ii == parts.length -1) {
                        n = parts[ii];
                    } else {
                        if (!sb.isEmpty()) {
                            sb.append(".");
                        }
                        sb.append(parts[ii]);
                    }
                }
                column.setTable(new Table(sb.toString()));
                column.setColumnName(n);
            }
        }
    }

    @Override
    protected String extractEntity(@NonNull Class<? extends E> type) {
        String entity = type.getSimpleName();
        if (type.isAnnotationPresent(Entity.class)) {
            Entity e = type.getAnnotation(Entity.class);
            if (!Strings.isNullOrEmpty(e.value())) {
                entity = e.value();
            }
        }
        return entity;
    }

    @Override
    protected Map<String, QueryField> extractFields(@NonNull Class<? extends E> type) throws Exception {
        Map<String, QueryField> map = new HashMap<>();
        extractFields(type, null, null, map);
        return map;
    }

    private void extractFields(Class<?> type,
                               String prefix,
                               String alias,
                               Map<String, QueryField> map) throws Exception {
        Field[] fields = ReflectionUtils.getAllFields(type);
        Preconditions.checkNotNull(fields);
        for (Field field : fields) {
            if (ignore(field)) continue;
            if (processField(field)) {
                String path = field.getName();
                if (!Strings.isNullOrEmpty(prefix)) {
                    path = String.format("%s.%s", prefix, field.getName());
                }
                String a = column(field);
                if (!Strings.isNullOrEmpty(alias)) {
                    a = String.format("%s.%s", alias, a);
                }
                QueryField qf = new QueryField()
                        .path(path)
                        .field(field)
                        .alias(a);
                map.put(qf.path(), qf);
            } else if (ReflectionUtils.isCollection(field)) {
                continue;
            } else if (ReflectionUtils.isSuperType(Map.class, field.getType())) {
                continue;
            } else {
                String p = field.getName();
                if (!Strings.isNullOrEmpty(prefix)) {
                    p = String.format("%s.%s", prefix, field.getName());
                }
                String a = column(field);
                if (!Strings.isNullOrEmpty(alias)) {
                    a = String.format("%s.%s", alias, a);
                }
                extractFields(field.getType(), p, a, map);
            }
        }
    }

    @Override
    protected Map<String, QueryField> extractKeyFields(@NonNull Class<? extends E> entityType,
                                                       @NonNull Class<? extends K> type) throws Exception {
        Field[] fields = ReflectionUtils.getAllFields(entityType);
        Preconditions.checkNotNull(fields);
        Class<?> idType = null;
        Field idField = null;
        for (Field field : fields) {
            if (field.isAnnotationPresent(Id.class)) {
                idField = field;
                idType = field.getType();
                break;
            }
        }
        if (idField == null) {
            throw new Exception(String.format("Primary Key not defined for type. [type=%s]",
                    entityType.getCanonicalName()));
        }
        if (processField(idField)) {
            QueryField qf = new QueryField()
                    .path(idField.getName())
                    .alias(column(idField))
                    .field(idField);
            return Map.of(qf.path(), qf);
        } else {
            if (!type.isAnnotationPresent(Entity.class)) {
                throw new Exception(String.format("Key type is not embeddable. [type=%s]", type.getCanonicalName()));
            }
            Field[] kfields = ReflectionUtils.getAllFields(idType);
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
        return field.getType().equals(ObjectId.class) ||
                ReflectionUtils.isPrimitiveTypeOrString(field) ||
                field.getType().isEnum() ||
                field.getType().equals(Date.class) ||
                field.getType().equals(Object.class);
    }

    private boolean ignore(Field field) {
        if (field.isAnnotationPresent(Transient.class)) {
            return true;
        } else return field.isAnnotationPresent(JsonIgnore.class);
    }

    private String column(Field field) {
        String name = field.getName();
        if (field.isAnnotationPresent(Property.class)) {
            Property p = field.getAnnotation(Property.class);
            if (!Strings.isNullOrEmpty(p.value())) {
                name = p.value();
            }
        }
        return name;
    }
}
