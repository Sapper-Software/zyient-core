package io.zyient.core.mapping.readers.impl.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.zyient.base.common.GlobalConstants;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.impl.json.JsonReadCursor;
import io.zyient.core.mapping.readers.settings.ContextReaderSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ContextInputReader extends InputReader {
    protected List<SourceMap> data;
    protected int readIndex = 0;

    @Override
    protected ReadCursor doOpen() throws IOException {
        Preconditions.checkState(settings() instanceof ContextReaderSettings);
        ContextReaderSettings readerSettings = (ContextReaderSettings) settings();
        Object value = contentInfo().get(readerSettings.getContextKey());
        if (value != null) {
            ObjectMapper mapper = GlobalConstants.getJsonMapper();
            if (value.getClass().isArray() || value instanceof ArrayList<?>) {
                List<Map<String, Object>> mapList = mapper.readValue(mapper.writeValueAsString(value), List.class);
                data = mapList.stream().map(c -> {
                    SourceMap sourceMap = new SourceMap();
                    sourceMap.putAll(c);
                    return sourceMap;
                }).toList();
            } else {
                Map<String, Object> map = mapper.readValue(mapper.writeValueAsString(value), Map.class);
                data = new ArrayList<>();
                SourceMap sourceMap = new SourceMap();
                sourceMap.putAll(map);
                data.add(sourceMap);
            }
        }

        return new JsonReadCursor(this, settings().getReadBatchSize());
    }

    @Override
    public List<SourceMap> fetchNextBatch() throws IOException {
        if (data != null && !data.isEmpty()) {
            if (readIndex < data.size()) {
                List<SourceMap> records = new ArrayList<>();
                int index = readIndex;
                for (int ii = 0; ii < settings().getReadBatchSize(); ii++) {
                    records.add(new SourceMap(data.get(index + ii)));
                    readIndex++;
                    if (readIndex >= data.size()) {
                        break;
                    }
                }
                return records;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
