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

package io.zyient.cdc.entity;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metadata {

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ColumnDef {
        private String name;
        private String type;
        private int position;
        private boolean nullable;
        private String charset;
        private String collation;
        private int size = -1;
        private int precision;
        private int scale;
        private Map<String, Object> properties;

        public boolean matches(@NonNull String type) {
            return this.type.compareToIgnoreCase(type) == 0;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class TableMetadata {
        private String catalog;
        private String schema;
        private String table;
        private Map<String, Integer> primaryKey;
        private List<ColumnDef> columns;
        private Map<String, Object> properties;

        public TableMetadata addColumn(@NonNull ColumnDef columnDef) {
            if (columns == null) {
                columns = new ArrayList<>();
            }
            columns.add(columnDef);

            return this;
        }

        public TableMetadata addKeyColumn(@NonNull ColumnDef columnDef, int position) {
            addColumn(columnDef);
            if (primaryKey == null) {
                primaryKey = new HashMap<>();
            }
            primaryKey.put(columnDef.name, position);

            return this;
        }

        public boolean isKeyColumn(@NonNull String name) {
            return primaryKey != null && primaryKey.containsKey(name);
        }

        public int keyPosition(@NonNull String name) {
            return primaryKey.get(name);
        }
    }
}
