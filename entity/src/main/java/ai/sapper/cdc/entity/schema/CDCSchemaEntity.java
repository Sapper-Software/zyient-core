package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.entity.model.EntityOptions;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CDCSchemaEntity extends SchemaEntity {

    public CDCSchemaEntity() {

    }

    public CDCSchemaEntity(@NonNull String domain, @NonNull String entity) {
        super(domain, entity);
    }

    public CDCSchemaEntity(@NonNull String domain,
                           @NonNull String entity,
                           @NonNull Map<String, Object> options) {
        super(domain, entity);
    }

    public CDCSchemaEntity(@NonNull SchemaEntity entity,
                           @NonNull Map<String, Object> options) {
        super(entity.getDomain(), entity.getEntity(), options);
    }

    public CDCSchemaEntity(@NonNull String domain,
                           @NonNull String entity,
                           @NonNull EntityOptions options) {
        super(domain, entity, options);
    }

    public CDCSchemaEntity(@NonNull SchemaEntity schemaEntity) {
        super(schemaEntity);
    }
}
