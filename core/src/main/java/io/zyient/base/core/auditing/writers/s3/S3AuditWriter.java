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

package io.zyient.base.core.auditing.writers.s3;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.auditing.JsonAuditRecord;
import io.zyient.base.core.auditing.writers.IAuditWriter;
import io.zyient.base.core.auditing.writers.local.FileAuditWriter;
import io.zyient.base.core.connections.aws.AwsS3Connection;
import io.zyient.base.core.connections.aws.S3Helper;
import io.zyient.base.core.processing.ProcessorState;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;

@Getter
@Accessors(fluent = true)
public class S3AuditWriter extends FileAuditWriter {
    private AwsS3Connection connection;
    private BaseEnv<?> env;

    @Override
    public IAuditWriter<JsonAuditRecord> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                              @NonNull BaseEnv<?> env,
                                              @NonNull Class<? extends JsonAuditRecord> recordType) throws ConfigurationException {
        Preconditions.checkState(!Strings.isNullOrEmpty(name()));
        this.env = env;
        try {
            ConfigReader reader = new ConfigReader(config, S3AuditWriterSettings.class);
            reader.read();
            settings = (S3AuditWriterSettings) reader.settings();
            setup();
            connection = env.connectionManager()
                    .getConnection(((S3AuditWriterSettings) settings).getConnection(),
                            AwsS3Connection.class);
            if (connection == null) {
                throw new ConfigurationException(String.format("AWS S3 connection not found. [name=%s]",
                        ((S3AuditWriterSettings) settings).getConnection()));
            }
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    protected FileHandle checkReCycle(FileHandle handle) throws Exception {
        long ts = handle.timestamp();
        handle = super.checkReCycle(handle);
        if (handle.timestamp() > ts && handle.archived() != null) {
            upload(handle.archived());
            if (!handle.archived().delete()) {
                DefaultLogger.warn(String.format("Failed to delete archived file. [path=%s]",
                        handle.archived().getAbsolutePath()));
            }
            handle.archived(null);
        } else {
            if (System.currentTimeMillis() - handle.timestamp()
                    >= ((S3AuditWriterSettings) settings).getSyncInterval().normalized()) {
                upload(handle.file());
                handle.timestamp(System.currentTimeMillis());
            }
        }
        return handle;
    }

    private void upload(File file) throws Exception {
        String path = getOutputPath(file);
        S3Helper.upload(connection.client(),
                ((S3AuditWriterSettings) settings).getBucket(),
                path,
                file);
    }

    private String getOutputPath(File file) {
        String path = PathUtils.formatPath(String.format("%s/%s/%s",
                ((S3AuditWriterSettings) settings).getRoot(),
                env.moduleInstance().id(),
                file.getAbsolutePath()));
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
}
