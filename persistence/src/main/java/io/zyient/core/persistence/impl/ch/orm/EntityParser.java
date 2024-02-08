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

package io.zyient.core.persistence.impl.ch.orm;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.beans.PropertyDef;
import jakarta.persistence.Entity;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

public class EntityParser {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ChColumn {
        private String name;
        private DataTypes dataType;
        private int position;
        private boolean nullable;
        private boolean keyColumn;
        private int precision;
        private int scale;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ChTable {
        private String schema;
        private String table;
        private final Map<String, ChColumn> columns = new HashMap<>();

        public ChTable addColumn(@NonNull ChColumn column) {
            columns.put(column.name, column);
            return this;
        }

        public ChColumn getColumn(@NonNull String name) {
            return columns.get(name);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class EntityField {
        private String path;
        private PropertyDef property;
        private DataTypes dataType;
        private ChColumn column;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ChEntity {
        private Class<?> entityType;
        private ChTable table;
        private final Map<String, EntityField> fields = new HashMap<>();

        public ChEntity addField(@NonNull EntityField field) {
            fields.put(field.path, field);
            return this;
        }
    }

    private final Map<String, ChTable> tables = new HashMap<>();
    private final Map<Class<?>, ChEntity> cache = new HashMap<>();

    public ChEntity get(@NonNull Class<?> type) throws Exception {
        synchronized (cache) {
            if (cache.containsKey(type)) {
                return cache.get(type);
            }
            return read(type);
        }
    }

    private ChEntity read(Class<?> type) throws Exception {
        if (!type.isAnnotationPresent(Entity.class)) {
            if (type.isAnnotationPresent(MappedSuperclass.class)) {
                throw new Exception(String.format("Entity instance must be specified: Type is a mapped super-class. [type=%s]",
                        type.getCanonicalName()));
            }
            throw new Exception(String.format("Not an entity type. [type=%s]", type.getCanonicalName()));
        }
        String table = null;
        if (type.isAnnotationPresent(Table.class)) {
            Table t = type.getAnnotation(Table.class);
            if (!Strings.isNullOrEmpty(t.name())) {
                table = t.name();
            }
        }
        if (Strings.isNullOrEmpty(table)) {
            table = type.getSimpleName().toLowerCase();
        }
        ChTable tab = tables.get(table);
        if (tab == null) {
            tab = readTable(table);
        }
        return null;
    }

    private ChTable readTable(String name) throws Exception {

        return null;
    }
}
