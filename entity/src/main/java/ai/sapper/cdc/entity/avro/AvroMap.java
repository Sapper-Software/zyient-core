package ai.sapper.cdc.entity.avro;

import ai.sapper.cdc.entity.types.DataType;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;

import java.sql.Types;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AvroMap<V> extends DataType<Map> {
    private Class<? extends V> inner;

    public AvroMap() {
        super("map", Map.class, Types.STRUCT);
    }

    @SuppressWarnings("unchecked")
    public AvroMap(@NonNull Schema.Field field) {
        super("map", Map.class, Types.STRUCT);
        Preconditions.checkArgument(field.schema().getType() == Schema.Type.MAP);
        Schema in = field.schema().getValueType();
        DataType<?> dt = AvroEntitySchema.getNativeDatatype(in);
        if (dt == null) {
            dt = AvroEntitySchema.RECORD;
        }
        this.inner = (Class<? extends V>) dt.getJavaType();
    }
}
