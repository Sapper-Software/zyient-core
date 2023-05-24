package ai.sapper.cdc.entity;

import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.core.connections.db.DbConnection;
import ai.sapper.cdc.entity.schema.EntitySchema;
import ai.sapper.cdc.entity.types.CustomDataTypeMapper;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class EntitySchemaGenerator {
    private DbConnection connection;
    private CustomDataTypeMapper mapper;
    
    public abstract EntitySchema generate(@NonNull SchemaEntity schemaEntity,
                                          @NonNull SchemaEntity targetEntity,
                                          Object... params) throws Exception;

}
