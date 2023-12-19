package io.zyient.core.mapping.readers.impl.separated;

import com.google.common.base.Preconditions;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.FlattenedInputReaderSettings;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class FlattenedInputReader extends InputReader {
    private final SeparatedInputReader reader;
    private Map<String, Boolean> headerFields;


    public FlattenedInputReader() {
        reader = new SeparatedInputReader();
    }

    @Override
    public ReadCursor open() throws IOException {
        Preconditions.checkState(settings() instanceof FlattenedInputReaderSettings);
        return reader.open();
    }

    @Override
    public List<SourceMap> nextBatch() throws IOException {

        return null;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
