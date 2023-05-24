package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.entity.model.EntityOptions;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class Domain {
    private String name;
    private EntityOptions defaultOptions;
    private long updatedTime;

    public Domain() {
        defaultOptions = new EntityOptions();
    }

    public Domain(@NonNull String name) {
        this.name = name;
        defaultOptions = new EntityOptions();
    }

    public Domain(@NonNull String name, @NonNull EntityOptions options) {
        this.name = name;
        this.defaultOptions = options;
    }

    public Domain(@NonNull String name, @NonNull Map<String, Object> options) {
        this.name = name;
        this.defaultOptions = new EntityOptions(options);
    }
}
