/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.readers.impl.positional;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.PositionalColumn;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.settings.PositionalReaderSettings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PositionalInputReader extends InputReader {
    private BufferedReader reader;
    private boolean EOF = false;

    @Override
    public ReadCursor open() throws IOException {
        Preconditions.checkState(settings() instanceof PositionalReaderSettings);
        try {
            reader = new BufferedReader(new FileReader(contentInfo().path()));
            if (((PositionalReaderSettings) settings()).isSkipHeader()) {
                String line = reader.readLine();
                if (line == null) {
                    EOF = true;
                } else if (!Strings.isNullOrEmpty(line)) {
                    List<String> header = parseHeader(line);
                    if (((PositionalReaderSettings) settings()).isValidateHeader()) {
                        int index = 0;
                        for (String key : header) {
                            if (index >= ((PositionalReaderSettings) settings()).getColumns().size()) {
                                break;
                            }
                            PositionalColumn c = (PositionalColumn) ((PositionalReaderSettings) settings())
                                    .getColumns()
                                    .get(index);
                            if (c.getName().compareToIgnoreCase(key) != 0) {
                                throw new Exception(String
                                        .format("Invalid Column in data: [position=%d][expected=%s][reported=%s]",
                                                index, c.getName(), key));
                            }
                            index++;
                        }
                    }
                    if (DefaultLogger.isTraceEnabled()) {
                        StringBuilder builder = new StringBuilder();
                        int index = 0;
                        for (String key : header) {
                            PositionalColumn c = (PositionalColumn) ((PositionalReaderSettings) settings())
                                    .getColumns()
                                    .get(index);
                            builder.append(String.format("[COLUMN=%s, REPORTED=%s]", c.getName(), key));
                            index++;
                        }
                        DefaultLogger.trace(builder.toString());
                    }
                }
            }
            return new PositionalReadCursor(this, settings().getReadBatchSize());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new IOException(ex);
        }
    }

    @Override
    public List<Map<String, Object>> nextBatch() throws IOException {
        if (EOF) return null;
        try {
            List<Map<String, Object>> records = new ArrayList<>();
            int count = 0;
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    EOF = true;
                    break;
                }
                if (Strings.isNullOrEmpty(line)) {
                    continue;
                }
                records.add(parse(line));
                count++;
                if (count >= settings().getReadBatchSize())
                    break;
            }
            if (records.isEmpty()) {
                return null;
            }
            return records;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private Map<String, Object> parse(String line) throws Exception {
        PositionalReaderSettings settings = (PositionalReaderSettings) settings();
        Map<String, Object> map = new HashMap<>();
        for (int ii = 0; ii < settings.getColumns().size(); ii++) {
            PositionalColumn column = (PositionalColumn) settings.getColumns().get(ii);
            int end = -1;
            if (column.getLength() != null) {
                end = column.getPosStart() + column.getLength();
            } else {
                end = column.getPosEnd();
            }
            String value = line.substring(column.getPosStart(), end);
            map.put(column.getName(), value.trim());
        }
        return map;
    }

    private List<String> parseHeader(String line) throws Exception {
        PositionalReaderSettings settings = (PositionalReaderSettings) settings();
        List<String> header = new ArrayList<>();
        for (int ii = 0; ii < settings.getColumns().size(); ii++) {
            PositionalColumn column = (PositionalColumn) settings.getColumns().get(ii);
            int end = -1;
            if (column.getLength() != null) {
                end = column.getPosStart() + column.getLength();
            } else {
                end = column.getPosEnd();
            }
            String value = line.substring(column.getPosStart(), end);
            header.add(value.trim());
        }
        return header;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
