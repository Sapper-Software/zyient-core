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

package io.zyient.base.core.auditing.readers.file;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.auditing.AuditCursor;
import io.zyient.base.core.auditing.JsonAuditRecord;
import io.zyient.base.core.auditing.JsonAuditSerDe;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@Accessors(fluent = true)
public class FileAuditCursor extends AuditCursor<JsonAuditRecord, String> {
    private List<File> files;
    private int index = 0;
    private BufferedReader reader = null;

    public FileAuditCursor(int currentPage) {
        super(currentPage);
    }

    public FileAuditCursor() {
    }

    @Override
    public AuditCursor<JsonAuditRecord, String> open() throws Exception {
        super.open();
        if (encryption().encrypted()) {
            if (!(serializer() instanceof JsonAuditSerDe)) {
                throw new Exception(String.format("Invalid serializer specified. [type=%s]",
                        serializer().getClass().getCanonicalName()));
            }
        }
        return this;
    }

    @Override
    protected List<JsonAuditRecord> next(int page) throws Exception {
        if (state == AuditCursorState.EOF || index >= files.size()) return null;
        int count = 0;
        List<JsonAuditRecord> records = new ArrayList<>(pageSize());
        while (count < pageSize()) {
            String line = reader.readLine();
            if (line == null) {
                reader = null;
                reader = getReader();
                if (reader == null) {
                    break;
                }
            }
            if (Strings.isNullOrEmpty(line)) continue;
            JsonAuditRecord record = JSONUtils.read(line, recordType());
            if (encryption().encrypted()) {
                JsonAuditSerDe serializer = (JsonAuditSerDe) serializer();
                String value = serializer.deserialize(record.getRecord(), String.class, encryption(), keyStore());
                record.setRecord(value);
            }
            records.add(record);
        }
        if (!records.isEmpty()) {
            return records;
        }
        return null;
    }

    private BufferedReader getReader() throws Exception {
        if (index == files.size()) return null;
        if (reader == null) {
            reader = new BufferedReader(new FileReader(files.get(index)));
            index++;
        }
        return reader;
    }

    @Override
    public void close() throws IOException {
        state = AuditCursorState.Closed;
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
