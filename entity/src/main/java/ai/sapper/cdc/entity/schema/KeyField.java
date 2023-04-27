package ai.sapper.cdc.entity.schema;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KeyField extends SchemaField {
    private short sequence;

    public KeyField() {
        setNullable(false);
    }

    public KeyField(@NonNull SchemaField field, short sequence) {
        super(field);
        this.sequence = sequence;
    }
}
