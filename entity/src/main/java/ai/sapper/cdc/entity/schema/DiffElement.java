package ai.sapper.cdc.entity.schema;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class DiffElement {
    private SchemaField field;
    private SchemaField current;
    private ESchemaOp op;

    public DiffElement(@NonNull SchemaField field,
                       @NonNull ESchemaOp op) {
        this.field = field;
        this.op = op;
    }

    public boolean isCompatible() {
        if (current != null) {
            return current().isCompatible(field);
        }
        return false;
    }
}
