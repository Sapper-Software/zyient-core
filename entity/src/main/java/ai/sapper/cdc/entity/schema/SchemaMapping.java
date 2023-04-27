package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.entity.EntitySchemaManager;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SchemaMapping extends BaseSchemaMapping {

    public BaseSchemaMapping read(@NonNull EntitySchemaManager schemaManager) throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(getEntitySchemaPath()));
        setEntitySchema(schemaManager.getEntitySchema(getEntitySchemaPath()));
        getEntitySchema().load();

        return this;
    }
}
