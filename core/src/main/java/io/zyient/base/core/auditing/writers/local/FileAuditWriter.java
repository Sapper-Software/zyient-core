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

package io.zyient.base.core.auditing.writers.local;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DateTimeUtils;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.auditing.AuditContext;
import io.zyient.base.core.auditing.Audited;
import io.zyient.base.core.auditing.JsonAuditRecord;
import io.zyient.base.core.auditing.writers.IAuditWriter;
import io.zyient.base.core.processing.ProcessorState;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class FileAuditWriter implements IAuditWriter<JsonAuditRecord> {
    public static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
    public static final String DEFAULT_FILE_NAME = "%s-audit.json";

    @Getter
    @Setter
    @Accessors(fluent = true)
    protected static class FileHandle {
        private File file;
        private FileOutputStream outputStream;
        private long size;
        private long timestamp;
        private File archived = null;
    }

    private final ProcessorState state = new ProcessorState();
    private Class<? extends JsonAuditRecord> recordType;
    private File directory;
    @Getter(AccessLevel.NONE)
    private FileHandle defaultWriter;
    protected FileAuditWriterSettings settings;
    @Getter(AccessLevel.NONE)
    private final Map<String, FileHandle> writers = new HashMap<>();
    private String name;

    @Override
    public IAuditWriter<JsonAuditRecord> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                              @NonNull BaseEnv<?> env,
                                              @NonNull Class<? extends JsonAuditRecord> recordType) throws ConfigurationException {
        Preconditions.checkState(!Strings.isNullOrEmpty(name));
        try {
            ConfigReader reader = new ConfigReader(config, FileAuditWriterSettings.class);
            reader.read();
            settings = (FileAuditWriterSettings) reader.settings();
            setup();
            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    protected void setup() throws Exception {
        directory = new File(settings.getDir());
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new Exception(String.format("Failed to create directory. [path=%s]",
                        directory.getAbsolutePath()));
            }
        }
        File outf = new File(PathUtils.formatPath(String.format("%s/" + DEFAULT_FILE_NAME,
                directory.getAbsolutePath(), name)));
        defaultWriter = new FileHandle();
        defaultWriter.file = outf;
        defaultWriter.size = outf.length();
        defaultWriter = checkReCycle(defaultWriter);
    }

    protected FileHandle checkReCycle(FileHandle handle) throws Exception {
        if (handle.size >= settings.getRolloverSize().normalized()) {
            if (handle.outputStream != null) {
                handle.outputStream.flush();
                handle.outputStream.close();
            }
            String dir = DateTimeUtils.now("yyyy/MM/dd");
            dir = PathUtils.formatPath(String.format("%s/%s", directory.getAbsolutePath(), dir));
            File d = new File(dir);
            if (!d.exists()) {
                if (!d.mkdirs()) {
                    throw new IOException(String.format("Failed to create directory. [path=%s]", d.getAbsolutePath()));
                }
            }
            String path = handle.file.getAbsolutePath();
            String name = FilenameUtils.getName(handle.file.getAbsolutePath());
            name = FilenameUtils.removeExtension(name);
            String prefix = DateTimeUtils.now("HH-mm");
            String nf = PathUtils.formatPath(String.format("%s/%s.%s.json", d.getAbsolutePath(), prefix, name));
            File tf = new File(nf);
            if (tf.exists()) {
                throw new IOException(String.format("Target file already exists: re-cycle size might be too small. [file=%s]",
                        tf.getAbsolutePath()));
            }
            if (!handle.file.renameTo(tf)) {
                throw new Exception(String.format("Failed to rename: [path=%s]", tf.getAbsolutePath()));
            }
            handle.archived = tf;
            handle.file = new File(path);
            handle.outputStream = new FileOutputStream(handle.file);
            handle.size = 0;
            handle.timestamp = System.currentTimeMillis();
        }
        return handle;
    }

    @Override
    public void write(@NonNull JsonAuditRecord record, Context context) throws Exception {
        state.check(ProcessorState.EProcessorState.Running);
        FileHandle handle = getOutputStream(context);
        byte[] data = JSONUtils.asBytes(record);
        Preconditions.checkNotNull(data);
        if (data.length > 0) {
            handle.outputStream.write(data);
            handle.size += data.length;
            handle.outputStream.write(NEWLINE);
            handle.size += NEWLINE.length;
        }
    }

    @Override
    public IAuditWriter<JsonAuditRecord> name(@NonNull String name) {
        this.name = name;
        return this;
    }

    @Override
    public String name() {
        return name;
    }

    private synchronized FileHandle getOutputStream(Context context) throws Exception {
        AuditContext ctx = get(context);
        Audited audited = null;
        Class<?> type = null;
        if (ctx != null) {
            audited = ctx.audited();
            type = ctx.type();
        }
        FileHandle handle = defaultWriter;
        if (audited != null) {
            if (audited.exclusive()) {
                String name = type.getSimpleName().toLowerCase();
                if (!writers.containsKey(name)) {
                    String path = PathUtils.formatPath(String.format("%s/%s.json", directory.getAbsolutePath(), name));
                    handle = new FileHandle();
                    handle.file = new File(path);
                    handle.outputStream = new FileOutputStream(handle.file);
                    if (handle.file.exists())
                        handle.size = handle.file.length();
                    else
                        handle.size = 0;
                    writers.put(name, handle);
                } else {
                    handle = writers.get(name);
                }
            }
        }
        return checkReCycle(handle);
    }

    private AuditContext get(Context context) {
        if (context instanceof AuditContext) {
            return (AuditContext) context;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (!state.hasError()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        if (defaultWriter != null) {
            defaultWriter.outputStream.close();
            defaultWriter = null;
        }
        for (String name : writers.keySet()) {
            FileHandle handle = writers.get(name);
            if (handle.outputStream != null) {
                handle.outputStream.close();
            }
        }
        writers.clear();
    }
}
