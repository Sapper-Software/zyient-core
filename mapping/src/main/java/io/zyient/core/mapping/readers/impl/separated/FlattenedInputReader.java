package io.zyient.core.mapping.readers.impl.separated;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.KeyValuePair;
import io.zyient.core.mapping.model.SourceMap;
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
    enum ParsingState {
        SectionStart,
        InHeader,
        RecordHeaderParsed,
        InRecordsSection
    }

    private Map<String, Boolean> sectionHeaderDef;
    private Map<String, String> sectionHeader;
    private Map<String, String> recordHeader;
    private final Queue<SourceMap> records = new LinkedBlockingQueue<>();
    private boolean EOF = false;
    private ParsingState parsingState = ParsingState.SectionStart;

    public FlattenedInputReader() {
    }

    @Override
    public ReadCursor open() throws IOException {
        Preconditions.checkState(settings() instanceof FlattenedInputReaderSettings);
        sectionHeaderDef = ((FlattenedInputReaderSettings) settings()).getFields();
        if (sectionHeaderDef == null || sectionHeaderDef.isEmpty()) {
            throw new IOException(String.format("Invalid configuration : Section Headers missing. [name=%s]",
                    settings().getName()));
        }
        return super.open();
    }

    @Override
    public List<SourceMap> nextBatch() throws IOException {
        if (EOF && records.isEmpty()) return null;
        if (!EOF && records.isEmpty()) {
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
        }
        return null;
    }

    private void validateSectionHeader() throws IOException {
        for (String field : sectionHeaderDef.keySet()) {
            if (!sectionHeader.containsKey(field)) {
                if (sectionHeaderDef.get(field)) {
                    throw new IOException(String.format("Mandatory header field missing. [field=%s]", field));
                }
            }
        }
    }

    private void fetch() throws IOException {
        FlattenedInputReaderSettings settings = (FlattenedInputReaderSettings) settings();
        while (true) {
            List<SourceMap> batch = super.nextBatch();
            if (batch == null || batch.isEmpty()) {
                EOF = true;
                return;
            }
            for (SourceMap record : batch) {
                if (record == null || record.isEmpty()) continue;
                if (record.size() == 1) {
                    KeyValuePair<String, String> kv = get(record);
                    Preconditions.checkNotNull(kv);
                    String value = kv.value().trim();
                    if (Strings.isNullOrEmpty(value)) continue;
                    if (settings.getSectionSeparator().compareTo(value) == 0) {
                        parsingState = ParsingState.SectionStart;
                        sectionHeader = null;
                        recordHeader = null;
                        continue;
                    }
                    if (parsingState == ParsingState.SectionStart
                            || parsingState == ParsingState.InHeader) {
                        if (parsingState == ParsingState.SectionStart) {
                            parsingState = ParsingState.InHeader;
                        }
                        if (sectionHeader == null) {
                            sectionHeader = new HashMap<>();
                        }
                        String[] parts = value.split(settings.getFieldSeparator());
                        if (parts.length != 2) {
                            continue;
                        }
                        String key = parts[0].trim();
                        String v = parts[1].trim();
                        if (Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(v)) {
                            throw new IOException(String
                                    .format("Invalid section header: Key and/or Value missing. [record=%s]",
                                            value));
                        }
                        sectionHeader.put(key, v);
                        continue;
                    }
                    throw new IOException(String.format("Invalid parsing state: [state=%s][record=%s]",
                            parsingState.name(), kv));
                } else {
                    if (parsingState == ParsingState.InRecordsSection
                            || parsingState == ParsingState.RecordHeaderParsed) {
                        if (recordHeader == null || recordHeader.isEmpty()) {
                            throw new IOException(String.format("Invalid parsing state: record header is null. [state=%s]",
                                    parsingState.name()));
                        }
                        if (parsingState == ParsingState.RecordHeaderParsed) {
                            parsingState = ParsingState.InRecordsSection;
                        }
                        record.putAll(sectionHeader);
                        for (String key : recordHeader.keySet()) {
                            String column = recordHeader.get(key);
                            if (record.containsKey(key)) {
                                Object value = record.remove(key);
                                record.put(column, value);
                            }
                        }
                        records.add(record);
                    } else if (parsingState == ParsingState.InHeader) {
                        validateSectionHeader();
                        recordHeader = new HashMap<>();
                        for (String key : record.keySet()) {
                            String column = (String) record.get(key);
                            recordHeader.put(key, column);
                        }
                        parsingState = ParsingState.RecordHeaderParsed;
                    }
                }
            }
            if (records.size() >= settings.getReadBatchSize()
                    && (parsingState == ParsingState.SectionStart || parsingState == ParsingState.InRecordsSection))
                break;
        }
    }

    private KeyValuePair<String, String> get(SourceMap map) {
        for (String key : map.keySet()) {
            return new KeyValuePair<>(key, (String) map.get(key));
        }
        return null;
    }
}
