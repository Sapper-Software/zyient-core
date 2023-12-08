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

package io.zyient.core.persistence;

import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.model.entity.NativeKey;
import io.zyient.base.common.utils.ReflectionHelper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;

import java.lang.reflect.Field;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class QueryParser<K extends IKey, E extends IEntity<K>> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class QueryField {
        private String path;
        private String alias;
        private Field field;
    }

    private final Class<? extends K> keyType;
    private final Class<? extends E> entityType;
    private final String entity;
    private final Map<String, QueryField> keyFields;
    private final Map<String, QueryField> entityFields;

    public QueryParser(@NonNull Class<? extends K> keyType,
                       @NonNull Class<? extends E> entityType) throws Exception {
        this.keyType = keyType;
        this.entityType = entityType;
        entity = extractEntity(entityType);
        keyFields = extractKeyFields(entityType, keyType);
        entityFields = extractFields(entityType);
    }

    public String parse(@NonNull AbstractDataStore.Q query) throws Exception {
        if (Strings.isNullOrEmpty(query.where())) {
            throw new Exception("Empty where clause not supported.");
        }
        StringBuilder builder = new StringBuilder("SELECT * FROM ");
        if (!Strings.isNullOrEmpty(query.where())) {
            builder.append(entity)
                    .append(" WHERE ")
                    .append(query.where());
        } else {
            builder.append(entity);
        }
        Select stmnt = (Select) CCJSqlParserUtil.parse(builder.toString());
        process(query, stmnt);
        return stmnt.toString();
    }

    protected boolean isNativeKey(@NonNull Field field) {
        if (ReflectionHelper.isSuperType(NativeKey.class, field.getType())) {
            return true;
        }
        return false;
    }

    protected abstract void process(@NonNull AbstractDataStore.Q query,
                                    @NonNull Select select) throws Exception;

    protected abstract String extractEntity(@NonNull Class<? extends E> type);

    protected abstract Map<String, QueryField> extractFields(@NonNull Class<? extends E> type) throws Exception;

    protected abstract Map<String, QueryField> extractKeyFields(@NonNull Class<? extends E> entityType,
                                                                @NonNull Class<? extends K> type) throws Exception;
}
