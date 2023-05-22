package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.entity.types.DataType;
import ai.sapper.cdc.entity.types.DataTypeUtils;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.sql.Types;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ArraySchemaField extends SchemaField {
    public static final DataType<List> ARRAY = new DataType<>("ARRAY", List.class, Types.ARRAY);

    private SchemaField field;

    public ArraySchemaField() {
        setDataType(ARRAY);
        setJdbcType(Types.ARRAY);
    }

    public ArraySchemaField(@NonNull ArraySchemaField source) {
        super(source);
        field = source.field;
    }

    /**
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            if (o instanceof ArraySchemaField) {
                return field.equals(((ArraySchemaField) o).field);
            }
        }
        return false;
    }

    /**
     * @return
     */
    @Override
    public int hashCode() {
        return Objects.hash(getName(), getDataType(), field.getDataType());
    }

    /**
     * @param target
     * @return
     */
    @Override
    public boolean isCompatible(@NonNull SchemaField target) {
        if (DataTypeUtils.isNullType(target)) return true;
        if (target instanceof ArraySchemaField) {
            return field.isCompatible(((ArraySchemaField) target).field);
        }
        return false;
    }
}
