package io.zyient.core.mapping.readers.impl.separated;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.FlattenedInputReaderSettings;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@Accessors(fluent = true)
public class FlattenedInputReader extends SeparatedInputReader {
    private Map<String, Boolean> sectionHeader;
    private Map<String, String> section;
    private final Queue<SourceMap> records = new LinkedBlockingQueue<>();
    private boolean EOF = false;

    public FlattenedInputReader() {
    }

    @Override
    public ReadCursor open() throws IOException {
        Preconditions.checkState(settings() instanceof FlattenedInputReaderSettings);
        sectionHeader = ((FlattenedInputReaderSettings) settings()).getFields();
        if (sectionHeader == null || sectionHeader.isEmpty()) {
            throw new IOException(String.format("Invalid configuration : Section Headers missing. [name=%s]",
                    settings().getName()));
        }
        return super.open();
    }

    @Override
    public List<SourceMap> nextBatch() throws IOException {
        if (EOF) return null;
        if (records.isEmpty()) {
            fetch();
        }
        if (!records.isEmpty()) {
            List<SourceMap> values = new ArrayList<>(settings().getReadBatchSize());
            for (int ii = 0; ii < settings().getReadBatchSize(); ii++) {
                SourceMap value = records.poll();
                if (value == null) {
                    break;
                }
                values.add(value);
            }
            return values;
        } else {
            EOF = true;
        }
        return null;
    }

    private void fetch() throws IOException {
        FlattenedInputReaderSettings settings = (FlattenedInputReaderSettings) settings();
        List<SourceMap> batch = super.nextBatch();
        if (batch == null) {
            EOF = true;
            return;
        }
        boolean inHeader = (section == null);
        while (true) {
            for (SourceMap map : batch) {
                if (map == null || map.isEmpty()) continue;
                if (map.size() == 1) {
                    String value = null;
                    for (String key : map.keySet()) {
                        value = (String) map.get(key);
                    }
                    if (Strings.isNullOrEmpty(value)) continue;
                    value = value.trim();
                    if (value.compareTo(settings.getSectionSeparator()) == 0) {
                        if (inHeader) {
                            throw new IOException("Unexpected section separator encountered...");
                        }
                        inHeader = true;
                        section = new HashMap<>();
                    }
                } else {
                    inHeader = false;
                }
            }
        }
    }
}
