package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.entity.model.EntityOptions;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;
import java.util.Objects;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class SchemaEntity {
    private String domain;
    private int group = -1;
    private String entity;
    private boolean enabled = false;
    private EntityOptions options;
    private long updatedTime;

    public SchemaEntity() {
        options = new EntityOptions();
    }

    public SchemaEntity(@NonNull String domain, @NonNull String entity) {
        this.domain = domain;
        this.entity = entity;
        options = new EntityOptions();
    }

    public SchemaEntity(@NonNull String domain,
                        @NonNull String entity,
                        @NonNull EntityOptions options) {
        this.domain = domain;
        this.entity = entity;
        this.options = options;
    }

    public SchemaEntity(@NonNull String domain,
                        @NonNull String entity,
                        @NonNull Map<String, Object> options) {
        this.domain = domain;
        this.entity = entity;
        this.options = new EntityOptions(options);
    }

    public SchemaEntity(@NonNull SchemaEntity source) {
        this.domain = source.getDomain();
        this.entity = source.getEntity();
        this.group = source.getGroup();
        this.options = new EntityOptions(source.options);
    }

    public EntityOptions withOptions(@NonNull Map<String, Object> options) {
        this.options = new EntityOptions(options);
        return this.options;
    }


    public EntityOptions addOption(@NonNull String option, @NonNull Object value) {
        if (options == null) {
            options = new EntityOptions();
        }
        options.put(option, value);
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaEntity that = (SchemaEntity) o;
        return domain.equals(that.domain) && Objects.equals(group, that.group) && entity.equals(that.entity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domain, group, entity);
    }

    @Override
    public String toString() {
        return String.format("[domain=%s][entity=%s][group=%d]", domain, entity, group);
    }

    public static String key(@NonNull SchemaEntity entity) {
        return key(entity.getDomain(), entity.getEntity());
    }

    public static String key(@NonNull String domain, @NonNull String entity) {
        return String.format("%s::%s", domain, entity);
    }
}
