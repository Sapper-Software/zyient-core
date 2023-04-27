package ai.sapper.cdc.entity.schema;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class EntityDiff {
    private Map<String, DiffElement> diff;
    private boolean hasDrop;
    private boolean hasAdd;
    private boolean hasModify;

    public boolean isEmpty() {
        if (diff != null) {
            return diff.isEmpty();
        }
        return true;
    }

    public DiffElement get(String name) {
        if (diff != null && !diff.isEmpty()) {
            return diff.get(name);
        }
        return null;
    }

    public DiffElement put(@NonNull SchemaField field,
                           @NonNull ESchemaOp op) {
        if (diff == null) {
            diff = new HashMap<>();
        }
        DiffElement e = new DiffElement(field, op);
        diff.put(field.getName(), e);
        switch (op) {
            case ADD:
                hasAdd = true;
                break;
            case DROP:
                hasDrop = true;
                break;
            default:
                hasModify = true;
        }
        return e;
    }

    public boolean hasDroppedFields() {
        return hasDrop;
    }

    public boolean hasNewFields() {
        return hasAdd;
    }

    public boolean droppedFieldsOnly() {
        return (hasDrop && !hasAdd && !hasModify);
    }

    public boolean newFieldsOnly() {
        return (hasAdd && !hasDrop && !hasModify);
    }

    public List<SchemaField> droppedFields() {
        if (hasDroppedFields()) {
            List<SchemaField> fields = new ArrayList<>();
            for (String name : diff.keySet()) {
                DiffElement e = diff.get(name);
                if (e.op() == ESchemaOp.DROP) fields.add(e.field());
            }
            return fields;
        }
        return null;
    }

    public List<SchemaField> newFields() {
        if (hasNewFields()) {
            List<SchemaField> fields = new ArrayList<>();
            for (String name : diff.keySet()) {
                DiffElement e = diff.get(name);
                if (e.op() == ESchemaOp.ADD) fields.add(e.field());
            }
            return fields;
        }
        return null;
    }

    public List<SchemaField> renamedFields() {
        List<SchemaField> fields = new ArrayList<>();
        for (String name : diff.keySet()) {
            DiffElement e = diff.get(name);
            if (e.op() == ESchemaOp.RENAME) fields.add(e.field());
        }
        if (!fields.isEmpty()) return fields;
        return null;
    }

    public List<SchemaField> modifiedFields() {
        List<SchemaField> fields = new ArrayList<>();
        for (String name : diff.keySet()) {
            DiffElement e = diff.get(name);
            if (e.op() == ESchemaOp.MODIFY) fields.add(e.field());
        }
        if (!fields.isEmpty())
            return fields;

        return null;
    }

    public boolean isCompatible() {
        if (diff != null && !diff.isEmpty()) {
            boolean ret = true;
            List<String> toRemove = new ArrayList<>();
            for (String key : diff.keySet()) {
                if (!diff.get(key).isCompatible()) {
                    ret = false;
                    continue;
                }
                toRemove.add(key);
            }
            if (!toRemove.isEmpty()) {
                for (String key : toRemove) {
                    diff.remove(key);
                }
            }
            if (diff != null && !diff.isEmpty()) {
                hasDrop = false;
                hasAdd = false;
                hasModify = false;
                for (String key : diff.keySet()) {
                    DiffElement de = diff.get(key);
                    switch (de.op()) {
                        case DROP:
                            hasDrop = true;
                            break;
                        case MODIFY:
                            hasModify = true;
                            break;
                        case ADD:
                            hasAdd = true;
                            break;
                    }
                }
            }
            return ret;
        }
        return true;
    }
}
