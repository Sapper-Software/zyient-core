package ai.sapper.cdc.entity.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class BaseSchemaMapping {
    @JsonIgnore
    public static final String IGNORE_MAPPING_STRING = "__IGNORE_FIELD__";

    @JsonIgnore
    private EntitySchema entitySchema;
    private Map<String, String> mappings = new HashMap<>();
    private String entitySchemaPath;
    private SchemaEntity targetEntity;

    public void createDefaultMappings() throws Exception {
        Preconditions.checkNotNull(entitySchema);
        for (String f : entitySchema.getFields().keySet()) {
            SchemaField field = entitySchema.get(f);
            addMapping(field.getName(), field.getName());
        }
    }

    public BaseSchemaMapping apply(@NonNull EntityDiff diff, boolean ignoreDrop) throws Exception {
        if (!diff.isEmpty()) {
            for (DiffElement de : diff.diff().values()) {
                switch (de.op()) {
                    case ADD:
                        addMapping(de.field().getName(), de.field().getName());
                        break;
                    case DROP:
                        if (!ignoreDrop) {
                            mappings.remove(de.field().getName());
                        }
                        break;
                }
            }
        }
        return this;
    }

    public BaseSchemaMapping addMapping(@NonNull String name, @NonNull String target) throws Exception {
        if (mappings == null) {
            mappings = new HashMap<>();
        }
        SchemaField sf = entitySchema.get(name);
        if (sf == null) {
            throw new Exception(String.format("Specified field not found. [name=%s]", name));
        }
        mappings.put(target, sf.getName());
        return this;
    }

    public SchemaField getTargetField(@NonNull String name) throws Exception {
        String tn = name;
        if (mappings.containsKey(name)) {
            tn = mappings.get(name);
            if (tn.compareToIgnoreCase(IGNORE_MAPPING_STRING) == 0) {
                return null;
            }
        }
        return entitySchema.get(tn);
    }

    public void validate() throws Exception {
        if (Strings.isNullOrEmpty(entitySchemaPath)) {
            throw new Exception(
                    String.format("Schema Path not set. [entity=%s]", entitySchema.getSchemaEntity().toString()));
        }
        for (String name : mappings.keySet()) {
            if (entitySchema.get(mappings.get(name)) == null) {
                throw new Exception(
                        String.format("Invalid Mapping: Schema Field not found. [field=%s][schema=%s]",
                                mappings.get(name), entitySchema.getSchemaEntity().toString()));
            }
        }
    }
}
