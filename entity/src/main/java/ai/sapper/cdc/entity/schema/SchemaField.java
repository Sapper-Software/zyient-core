package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.entity.DataType;
import ai.sapper.cdc.entity.DataTypeUtils;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Getter
@Setter
@ToString
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class SchemaField implements Comparable<SchemaField> {
    private String name;
    private DataType<?> dataType;
    private Object defaultVal;
    private int position;
    private boolean nullable = true;
    private boolean deleted = false;
    private String charset;
    private int jdbcType;

    public SchemaField() {
    }

    public SchemaField(@NonNull SchemaField field) {
        this.name = field.name;
        this.dataType = field.dataType;
        this.defaultVal = field.defaultVal;
        this.position = field.position;
        this.nullable = field.nullable;
    }

    @Override
    public boolean equals(Object o) {
        Preconditions.checkState(!Strings.isNullOrEmpty(name));
        Preconditions.checkNotNull(dataType);

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaField that = (SchemaField) o;
        return name.equals(that.name) && dataType.equals(that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType);
    }

    /**
     * @param field
     * @return
     */
    @Override
    public int compareTo(@NonNull SchemaField field) {
        if (position >= 0) {
            return position - field.position;
        }
        return name.compareTo(field.getName());
    }

    public boolean isCompatible(@NonNull SchemaField target) {
        if (DataTypeUtils.isNullType(target)) return true;
        return DataTypeUtils.isCompatible(target.getDataType(), getDataType());
    }
}
