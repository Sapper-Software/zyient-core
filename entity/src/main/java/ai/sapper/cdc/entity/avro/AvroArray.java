package ai.sapper.cdc.entity.avro;

import ai.sapper.cdc.entity.types.DataType;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;

import java.sql.Types;
import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AvroArray<I> extends DataType<List> {
    private Class<? extends I> inner;

    public AvroArray() {
        super("array", List.class, Types.ARRAY);
    }

    @SuppressWarnings("unchecked")
    public AvroArray(@NonNull Schema.Field field) {
        super("array", List.class, Types.ARRAY);
        Preconditions.checkArgument(field.schema().getType() == Schema.Type.ARRAY);
        Schema in = field.schema().getElementType();
        DataType<?> dt = AvroEntitySchema.getNativeDatatype(in);
        if (dt == null) {
            dt = AvroEntitySchema.RECORD;
        }
        this.inner = (Class<? extends I>) dt.getJavaType();
    }
}
