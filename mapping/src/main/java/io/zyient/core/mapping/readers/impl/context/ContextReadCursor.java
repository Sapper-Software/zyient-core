package io.zyient.core.mapping.readers.impl.context;

import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import lombok.NonNull;

public class ContextReadCursor extends ReadCursor {
    public ContextReadCursor(@NonNull InputReader reader, int batchSize) {
        super(reader, batchSize);
    }
}
