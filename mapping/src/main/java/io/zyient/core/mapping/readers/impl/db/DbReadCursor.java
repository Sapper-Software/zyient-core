package io.zyient.core.mapping.readers.impl.db;

import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.core.mapping.readers.ReadCursor;
import lombok.NonNull;

public class DbReadCursor<K extends IKey, E extends IEntity<K>> extends ReadCursor {

    public DbReadCursor(@NonNull DbInputReader reader,
                        int batchSize) {
        super(reader, batchSize);
    }
}
