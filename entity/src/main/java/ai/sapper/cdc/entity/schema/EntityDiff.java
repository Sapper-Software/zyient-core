/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
