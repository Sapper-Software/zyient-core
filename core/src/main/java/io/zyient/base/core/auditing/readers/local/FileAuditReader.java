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

package io.zyient.base.core.auditing.readers.local;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DateTimeUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.auditing.*;
import io.zyient.base.core.auditing.writers.local.FileAuditWriter;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class FileAuditReader extends AuditReader<JsonAuditRecord, String> {
    public static final String DATE_REGEX = ".*/(\\d{4})/(\\d{2})/(\\d{2})/(\\d{2})-(\\d{2}).*";

    private File directory;

    public FileAuditReader() {
        super(FileAuditReaderSettings.class);
    }

    @Override
    public AuditReader<JsonAuditRecord, String> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        super.init(config, env);
        try {
            return setup();
        } catch (Exception ex) {
            state().error(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public AuditReader<JsonAuditRecord, String> init(@NonNull HierarchicalConfiguration<ImmutableNode> config)
            throws ConfigurationException {
        super.init(config);
        try {
            return setup();
        } catch (Exception ex) {
            state().error(ex);
            throw new ConfigurationException(ex);
        }
    }

    private FileAuditReader setup() throws Exception {
        FileAuditReaderSettings settings = (FileAuditReaderSettings) settings();
        directory = new File(settings.getDir());
        if (!directory.exists()) {
            throw new IOException(String.format("Directory not found. [path=%s]", directory.getAbsolutePath()));
        }
        return this;
    }

    @Override
    public List<JsonAuditRecord> read(int page,
                                      int batchSize,
                                      @NonNull EncryptionInfo encryption,
                                      Class<?> type,
                                      Context context) throws Exception {
        int start = page * batchSize;
        File current = current(type);
        if (current != null) {
            FileAuditCursor cursor = (FileAuditCursor) new FileAuditCursor(page)
                    .files(List.of(current))
                    .encryption(encryption)
                    .recordType(JsonAuditRecord.class)
                    .pageSize(batchSize)
                    .keyStore(env().keyStore())
                    .serializer(new JsonAuditSerDe());

        }
        return null;
    }

    @Override
    public AuditCursor<JsonAuditRecord, String> read(long timeStart,
                                                     long timeEnd,
                                                     @NonNull EncryptionInfo encryption,
                                                     Class<?> type,
                                                     Context context) throws Exception {
        List<File> files = fetchFiles(timeStart, timeEnd, type);
        if (files != null && !files.isEmpty()) {
            FileAuditCursor cursor = (FileAuditCursor) new FileAuditCursor(0)
                    .files(files)
                    .encryption(encryption)
                    .recordType(JsonAuditRecord.class)
                    .keyStore(env().keyStore())
                    .serializer(new JsonAuditSerDe());

            return cursor.open();
        }
        return null;
    }

    private List<File> fetchFiles(long start,
                                  long end,
                                  Class<?> type) throws Exception {
        DateTime dStart = null;
        if (start > 0) {
            dStart = new DateTime(start);
        }
        DateTime dEnd = null;
        if (end > 0) {
            if (end < start) {
                throw new Exception(String.format("Invalid date range: [start=%d, end=%d]", start, end));
            }
            dEnd = new DateTime(end);
        }
        if (dStart == null && dEnd == null) {
            File current = current(type);
            if (current != null) {
                return List.of(current);
            }
        } else {
            List<File> files = new ArrayList<>();
            if (dEnd == null) {
                File current = current(type);
                if (current != null) {
                    files.add(current);
                }
            }
            Collection<File> paths = FileUtils.listFiles(directory, new String[]{"json"}, true);
            if (paths != null && !paths.isEmpty()) {
                Pattern pattern = Pattern.compile(DATE_REGEX);
                for (File path : paths) {
                    Matcher m = pattern.matcher(path.getAbsolutePath());
                    if (m.matches()) {
                        m = m.reset();
                        String date = String.format("%s/%s/%s %s:%s",
                                m.group(1),
                                m.group(2),
                                m.group(3),
                                m.group(4),
                                m.group(5));
                        DateTime dt = DateTimeUtils.parse(date, "yyyy/MM/dd HH:mm");
                        if (dStart == null || dt.isAfter(dStart)) {
                            if (dEnd == null || dt.isBefore(dEnd)) {
                                files.add(path);
                            }
                        }
                    }
                }
            }
            if (!files.isEmpty()) {
                return files;
            }
        }
        return null;
    }


    private File current(Class<?> type) {
        boolean exclusive = false;
        if (type != null && type.isAnnotationPresent(Audited.class)) {
            Audited audited = type.getAnnotation(Audited.class);
            exclusive = audited.exclusive();
        }
        if (type == null || !exclusive) {
            File file = new File(PathUtils.formatPath(String.format("%s/" + FileAuditWriter.DEFAULT_FILE_NAME,
                    directory.getAbsolutePath(), name())));
            if (file.exists()) {
                return file;
            } else {
                DefaultLogger.error(String.format("Default Audit Log file not found. [path=%s]",
                        file.getAbsolutePath()));
            }
        } else {
            String name = type.getSimpleName().toLowerCase();
            String path = PathUtils.formatPath(String.format("%s/%s.json", directory.getAbsolutePath(), name));
            File file = new File(path);
            if (file.exists()) {
                return file;
            } else {
                DefaultLogger.warn(String.format("No audit file found for type. [type=%s][path=%s]",
                        type.getCanonicalName(), file.getAbsolutePath()));
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
