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

package io.zyient.core.filesystem.sync.s3.process;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.aws.AwsS3Connection;
import io.zyient.base.core.connections.aws.S3Helper;
import io.zyient.core.filesystem.sync.s3.model.S3Event;
import io.zyient.core.filesystem.sync.s3.model.S3EventType;
import io.zyient.core.messaging.InvalidMessageError;
import io.zyient.core.messaging.MessageProcessingError;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class S3EventTestHandler implements S3EventHandler {
    private AwsS3Connection connection;
    private final Map<String, Boolean> received = new HashMap<>();
    private int receivedCount = 0;
    private int processedCount = 0;

    @Override
    public S3EventHandler init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                               @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            HierarchicalConfiguration<ImmutableNode> node = config.configurationAt(S3EventHandler.__CONFIG_PATH);
            String c = node.getString("connection");
            if (Strings.isNullOrEmpty(c)) {
                throw new Exception("S3 Connection not specified. [config=connection]");
            }
            connection = env.connectionManager()
                    .getConnection(c, AwsS3Connection.class);
            if (connection == null) {
                throw new Exception(String.format("S3 Connection not found. [name=%s]", c));
            }
            if (!connection.isConnected()) {
                connection.connect();
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void handle(@NonNull S3Event event) throws Exception {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        try {
            receivedCount++;
            if (event.getName().getType() != S3EventType.ObjectCreated) {
                DefaultLogger.info(String.format("Received event: [%s]", event.getName().toString()));
                throw new InvalidMessageError(event.getMessageId(), String.format("Event type not handled. [type=%s]",
                        event.getName().getType().name()));
            }
            if (event.getData().getSize() <= 0) {
                DefaultLogger.info(String.format("Received event with Zero size: [key=%s]", event.getData().getKey()));
            }

            S3Client client = connection.client();
            if (!S3Helper.exists(client, event.getBucket().getName(), event.getData().getKey())) {
                DefaultLogger.warn(String.format("Source file not found. [bucket=%s][key=%s]",
                        event.getBucket().getName(), event.getData().getKey()));
                return;
            }
            File output = S3Helper.download(client, event.getBucket().getName(), event.getData().getKey());
            if (output == null || !output.exists()) {
                throw new Exception(String.format("Failed to download S3 file. [bucket=%s][key=%s]",
                        event.getBucket().getName(), event.getData().getKey()));
            }
            received.put(event.getData().getKey(), true);
            processedCount++;
            DefaultLogger.info(String.format("Downloaded file from S3. [path=%s]", output.getAbsolutePath()));
            DefaultLogger.info(String.format("Event counts = [received=%d][processed=%d], [size=%d]",
                    receivedCount, processedCount, received.size()));
        } catch (InvalidMessageError | MessageProcessingError me) {
            throw me;
        } catch (Throwable t) {
            throw new Exception(t);
        }
    }
}
