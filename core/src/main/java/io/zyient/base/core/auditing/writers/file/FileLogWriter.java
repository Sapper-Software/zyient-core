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

package io.zyient.base.core.auditing.writers.file;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DateTimeUtils;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.auditing.AuditContext;
import io.zyient.base.core.auditing.Audited;
import io.zyient.base.core.auditing.IAuditWriter;
import io.zyient.base.core.auditing.JsonAuditRecord;
import io.zyient.base.core.processing.ProcessorState;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileLogWriter implements IAuditWriter<JsonAuditRecord> {
    private static class FileHandle {
        private File file;
        private FileOutputStream outputStream;
        private long size;
    }

    private final ProcessorState state = new ProcessorState();
    private Class<? extends JsonAuditRecord> recordType;
    private File directory;
    private FileHandle defaultWriter;
    private FileLogWriterSettings settings;
    private final Map<String, FileHandle> writers = new HashMap<>();

    @Override
    public IAuditWriter<JsonAuditRecord> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                              @NonNull BaseEnv<?> env,
                                              @NonNull Class<? extends JsonAuditRecord> recordType) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(config, FileLogWriterSettings.class);
            reader.read();
            settings = (FileLogWriterSettings) reader.settings();
            directory = new File(settings.getDir());
            if (!directory.exists()) {
                if (!directory.mkdirs()) {
                    throw new Exception(String.format("Failed to create directory. [path=%s]",
                            directory.getAbsolutePath()));
                }
            }
            File outf = new File(PathUtils.formatPath(String.format("%s/%s-audit.json",
                    directory.getAbsolutePath(), settings.getName())));
            defaultWriter = new FileHandle();
            defaultWriter.file = outf;
            defaultWriter.size = outf.length();
            defaultWriter = checkReCycle(defaultWriter);
            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    private FileHandle checkReCycle(FileHandle handle) throws Exception {
        if (handle.size >= settings.getRolloverSize().normalized()) {
            if (handle.outputStream != null) {
                handle.outputStream.flush();
                handle.outputStream.close();
            }
            String dir = FilenameUtils.getFullPath(handle.file.getAbsolutePath());
            String path = handle.file.getAbsolutePath();
            String name = FilenameUtils.getName(handle.file.getAbsolutePath());
            name = FilenameUtils.removeExtension(name);
            String prefix = DateTimeUtils.now("yyyy-MM-dd.HH-mm");
            String nf = PathUtils.formatPath(String.format("%s/%s.%s.json", dir, prefix, name));
            if (!handle.file.renameTo(new File(nf))) {
                throw new Exception(String.format("Failed to rename: [path=%s]", nf));
            }
            handle.file = new File(path);
            handle.outputStream = new FileOutputStream(handle.file);
            handle.size = 0;
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
        }
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
                    handle.size = handle.file.length();
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
