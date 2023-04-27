package ai.sapper.cdc.entity;

import ai.sapper.cdc.common.schema.SchemaEntity;
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
    private EntityOptions options;
    private long updatedTime;

    public CDCSchemaEntity() {
        options = new EntityOptions();
    }

    public CDCSchemaEntity(@NonNull String domain, @NonNull String entity) {
        super(domain, entity);
        options = new EntityOptions();
    }

    public CDCSchemaEntity(@NonNull String domain,
                           @NonNull String entity,
                           @NonNull Map<String, Object> options) {
        super(domain, entity);
        this.options = new EntityOptions(options);
    }

    public CDCSchemaEntity(@NonNull SchemaEntity entity,
                           @NonNull Map<String, Object> options) {
        super(entity.getDomain(), entity.getEntity());
        this.options = new EntityOptions(options);
    }

    public CDCSchemaEntity(@NonNull String domain,
                           @NonNull String entity,
                           @NonNull EntityOptions options) {
        super(domain, entity);
        this.options = new EntityOptions(options);
    }

    public CDCSchemaEntity(@NonNull SchemaEntity entity,
                           @NonNull EntityOptions options) {
        super(entity.getDomain(), entity.getEntity());
        this.options = new EntityOptions(options);
    }

    public EntityOptions withOptions(@NonNull Map<String, Object> options) {
        this.options = new EntityOptions(options);
        return this.options;
    }

    public CDCSchemaEntity(@NonNull SchemaEntity schemaEntity) {
        setDomain(schemaEntity.getDomain());
        setEntity(schemaEntity.getEntity());
        setGroup(schemaEntity.getGroup());
        setEnabled(schemaEntity.isEnabled());

        if (schemaEntity instanceof CDCSchemaEntity) {
            if (((CDCSchemaEntity) schemaEntity).options != null) {
                options = new EntityOptions(((CDCSchemaEntity) schemaEntity).options);
            }
        }
        updatedTime = System.currentTimeMillis();
    }

    public EntityOptions addOption(@NonNull String option, @NonNull Object value) {
        if (options == null) {
            options = new EntityOptions();
        }
        options.put(option, value);
        return options;
    }

    public CDCSchemaEntity copy(@NonNull SchemaEntity entity) {
        CDCSchemaEntity e = new CDCSchemaEntity(this);
        e.setDomain(entity.getDomain());
        e.setEntity(entity.getEntity());

        return e;
    }
}
