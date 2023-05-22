package ai.sapper.cdc.entity.types;

import ai.sapper.cdc.entity.Metadata;
import ai.sapper.cdc.entity.schema.SchemaField;
import lombok.NonNull;
import org.apache.avro.Schema;

public interface CustomDataTypeMapper {
    Schema.Type getAvroType(@NonNull DataType<?> dataType);

    Schema.Type getAvroType(@NonNull SchemaField field);
    DataType<?> getDataType(@NonNull Metadata.ColumnDef column);
}
