package ai.sapper.cdc.entity.types;

import ai.sapper.cdc.common.schema.SchemaEntity;
import lombok.NonNull;

public class BadEntityRecordError extends Exception {
    private static final String __PREFIX = "Invalid Entity Record: %s. [entity=%s]";

    public BadEntityRecordError(@NonNull SchemaEntity schemaEntity,
                                @NonNull String msg) {
        super(String.format(__PREFIX, msg, schemaEntity.toString()));
    }

    public BadEntityRecordError(@NonNull SchemaEntity schemaEntity,
                                @NonNull String msg,
                                @NonNull Throwable t) {
        super(String.format(__PREFIX, msg, schemaEntity.toString()), t);
    }
}
