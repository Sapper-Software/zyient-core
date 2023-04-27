package ai.sapper.cdc.entity;

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
