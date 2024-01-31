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

package io.zyient.base.core.connections.aws;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.MessageConnection;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.aws.AwsSQSConnectionSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class AwsSQSConnection extends MessageConnection {
    public static final int SQS_MAX_BATCH_SIZE = 10;

    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    protected AwsSQSConnectionConfig config;
    protected SqsClient client;
    protected BaseEnv<?> env;
    private long connectedTimestamp = 0;

    @Override
    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            if (state.isConnected()) {
                close();
            }
            state.clear();
            config = new AwsSQSConnectionConfig(xmlConfig);
            config.read();

            settings = (AwsSQSConnectionSettings) config.settings();
            settings.validate();
            this.env = env;
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    public SqsClient client() throws ConnectionError {
        Preconditions.checkState(state.isConnected());
        synchronized (this) {
            try {
                long delta = System.currentTimeMillis() - connectedTimestamp;
                if (delta > ((AwsSQSConnectionSettings) settings).getResetTimeout().normalized()) {
                    client.close();
                    reconnect();
                }
                connectedTimestamp = System.currentTimeMillis();
                return client;
            } catch (Exception ex) {
                state.error(ex);
                DefaultLogger.stacktrace(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            CuratorFramework client = connection.client();
            String zkPath = new PathUtils.ZkPathBuilder(path)
                    .withPath(AwsSQSConnectionConfig.__CONFIG_PATH)
                    .build();
            ZkConfigReader reader = new ZkConfigReader(client, AwsSQSConnectionSettings.class);
            if (!reader.read(zkPath)) {
                throw new ConnectionError(
                        String.format("Chronicle Connection settings not found. [path=%s]", zkPath));
            }
            settings = (AwsSQSConnectionSettings) reader.settings();
            settings.validate();
            this.env = env;
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof AwsSQSConnectionSettings);
        try {
            if (state.isConnected()) {
                close();
            }
            state.clear();
            this.settings = (AwsSQSConnectionSettings) settings;
            this.settings.validate();
            this.env = env;
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(settings instanceof AwsSQSConnectionSettings);
        if (!state.isConnected()) {
            reconnect();
            state.setState(EConnectionState.Connected);
        }
        return this;
    }

    private void reconnect() {
        Region region = Region.of(((AwsSQSConnectionSettings) settings).getRegion());
        client = SqsClient.builder()
                .region(region)
                .build();
    }

    @Override
    public Throwable error() {
        return state.getError();
    }

    @Override
    public EConnectionState connectionState() {
        return state.getState();
    }

    @Override
    public boolean isConnected() {
        return state.isConnected();
    }

    @Override
    public String path() {
        return AwsSQSConnectionConfig.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return EConnectionType.sqs;
    }

    @Override
    public void close() throws IOException {
        client.close();
        client = null;
        if (state.isConnected()) {
            state.setState(EConnectionState.Closed);
        }
    }

    public static class AwsSQSConnectionConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "aws.sqs";

        public AwsSQSConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, AwsSQSConnectionSettings.class);
        }

        public AwsSQSConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                      @NonNull String path,
                                      @NonNull Class<? extends AwsSQSConnectionSettings> settingsType) {
            super(config, path, settingsType);
        }
    }
}
