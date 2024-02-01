/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.readers.impl.separated;

import com.google.common.base.Preconditions;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.SeparatedReaderSettings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SeparatedInputReader extends InputReader {
    private CSVFormat format = null;
    private CSVParser parser = null;
    private Reader reader;
    private Iterator<CSVRecord> iterator;

    @Override
    public ReadCursor open() throws IOException {
        Preconditions.checkState(settings() instanceof SeparatedReaderSettings);
        try {
            format = getReaderFormat(((SeparatedReaderSettings) settings()).getType());
            format = ((SeparatedReaderSettings) settings()).setup(format);
            reader = getReader(contentInfo().path());
            parser = new CSVParser(reader, format);
            iterator = parser.stream().iterator();
            return new SeparatedReadCursor(this, settings().getReadBatchSize());
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected Reader getReader(File file) throws IOException {
        return new FileReader(file);
    }

    private CSVFormat getReaderFormat(SeparatedReaderTypes type) {
        CSVFormat format = null;
        switch (type) {
            case EXCEL -> {
                format = CSVFormat.EXCEL;
            }
            case TDF -> {
                format = CSVFormat.TDF;
            }
            case MYSQL -> {
                format = CSVFormat.MYSQL;
            }
            case RFC_4180 -> {
                format = CSVFormat.RFC4180;
            }
            case MONGO_CSV -> {
                format = CSVFormat.MONGODB_CSV;
            }
            case MONGO_TSV -> {
                format = CSVFormat.MONGODB_TSV;
            }
            case POSTGRES_CSV -> {
                format = CSVFormat.POSTGRESQL_CSV;
            }
            case POSTGRES_TEXT -> {
                format = CSVFormat.POSTGRESQL_TEXT;
            }
            default -> {
                format = CSVFormat.DEFAULT;
            }
        }
        return format;
    }

    @Override
    public List<SourceMap> nextBatch() throws IOException {
        Preconditions.checkNotNull(parser);
        SeparatedReaderSettings settings = (SeparatedReaderSettings) settings();
        if (!parser.isClosed()) {
            List<SourceMap> batch = new ArrayList<>();
            int count = 0;
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                SourceMap data = new SourceMap(record.size());
                if (settings.getHasHeader()) {
                    Map<String, String> recordMap = record.toMap();
                    for (String key : recordMap.keySet()) {
                        data.put(key, recordMap.get(key));
                    }
                } else {
                    for (int ii = 0; ii < record.size(); ii++) {
                        String key = String.format("%s%d", settings.getColumnPrefix(), ii);
                        if (settings.getHeaders() != null) {
                            if (ii < settings.getHeaders().size()) {
                                key = settings.getHeaders()
                                        .get(ii)
                                        .getName();
                            }
                        }
                        String value = record.get(ii);
                        data.put(key, value);
                    }
                }
                batch.add(data);
                count++;
                if (count >= settings().getReadBatchSize())
                    break;
            }
            return batch;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (format != null) {
            format = null;
        }
        if (parser != null) {
            if (!parser.isClosed()) {
                parser.close();
            }
            parser = null;
        }
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }


}
