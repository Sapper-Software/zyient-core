package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.entity.DataType;
import ai.sapper.cdc.entity.DataTypeUtils;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class RecordSchemaField extends SchemaField {
    public static final DataType<Object> RECORD = new DataType<>("RECORD", Object.class, Types.STRUCT);
    private String namespace;
    private Map<String, SchemaField> fields;
    private String id;

    public RecordSchemaField() {
        setDataType(RECORD);
        setJdbcType(Types.STRUCT);
        id = UUID.randomUUID().toString();
    }

    public RecordSchemaField(@NonNull RecordSchemaField source) {
        super(source);
        if (source.fields != null) {
            fields = new HashMap<>(source.fields);
        }
        this.id = source.id;
    }

    public void add(@NonNull SchemaField field) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(field.getName()));
        if (fields == null) {
            fields = new LinkedHashMap<>();
        }
        fields.put(field.getName(), field);
    }

    public SchemaField getField(@NonNull String name) {
        if (fields != null) {
            return fields.get(name);
        }
        return null;
    }

    public boolean remove(@NonNull String name) {
        if (fields != null) {
            return (fields.remove(name) != null);
        }
        return false;
    }

    public boolean hasField(@NonNull SchemaField field) {
        if (fields != null && fields.containsKey(field.getName())) {
            SchemaField f = fields.get(field.getName());
            return f.equals(field);
        }
        return false;
    }

    public int size() {
        if (fields != null) {
            return fields.size();
        }
        return 0;
    }

    /**
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            if (o instanceof RecordSchemaField) {
                RecordSchemaField rs = (RecordSchemaField) o;
                if (size() == rs.size()) {
                    if (fields != null && !fields.isEmpty()) {
                        for (String name : fields.keySet()) {
                            SchemaField sf = fields.get(name);
                            if (!rs.hasField(sf)) return false;
                        }
                        for (String name : rs.fields.keySet()) {
                            SchemaField sf = rs.fields.get(name);
                            if (!hasField(sf)) return false;
                        }
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * @return
     */
    @Override
    public int hashCode() {
        if (fields != null && !fields.isEmpty()) {
            return fields.hashCode();
        }
        return super.hashCode();
    }

    /**
     * @param target
     * @return
     */
    @Override
    public boolean isCompatible(@NonNull SchemaField target) {
        if (DataTypeUtils.isNullType(target)) return true;
        if (target instanceof RecordSchemaField) {
            if (fields.size() != ((RecordSchemaField) target).fields.size()) return false;
            RecordSchemaField rsf = (RecordSchemaField) target;
            for (String key : fields.keySet()) {
                SchemaField cf = fields.get(key);
                SchemaField tf = rsf.getField(key);
                if (tf == null) return false;
                if (!cf.isCompatible(tf)) return false;
            }
            return true;
        }
        return false;
    }
}
