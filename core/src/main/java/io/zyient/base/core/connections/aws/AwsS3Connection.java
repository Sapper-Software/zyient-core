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
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.aws.auth.S3StorageAuth;
import io.zyient.base.core.connections.aws.auth.S3StorageAuthSettings;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.aws.AwsS3Settings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.io.IOException;
import java.net.URI;

@Getter
@Accessors(fluent = true)
public class AwsS3Connection implements Connection {
    @Getter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();
    private S3Client client;
    private AwsS3Settings settings;
    private BaseEnv<?> env;
    @Getter(AccessLevel.NONE)
    private S3StorageAuth auth;
    private long connectedTimestamp = 0;

    @Override
    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            ConfigReader reader = new ConfigReader(config, path(), AwsS3Settings.class);
            reader.read();
            settings = (AwsS3Settings) reader.settings();
            if (ConfigReader.checkIfNodeExists(reader.config(), S3StorageAuthSettings.__CONFIG_PATH)) {
                HierarchicalConfiguration<ImmutableNode> ac =
                        reader.config().configurationAt(S3StorageAuthSettings.__CONFIG_PATH);
                Class<? extends S3StorageAuth> clazz = (Class<? extends S3StorageAuth>) ConfigReader.readType(ac);
                auth = clazz.getDeclaredConstructor()
                        .newInstance();
                auth.init(reader.config(), env.keyStore());
                settings.setAuthHandler(clazz);
                settings.setAuthSettings(auth.settings());
            }
            settings.validate();
            return setup(settings, env);
        } catch (Exception ex) {
            state.error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConnectionError(ex);
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
                    .withPath(path)
                    .build();
            ZkConfigReader reader = new ZkConfigReader(client, AwsS3Settings.class);
            if (!reader.read(zkPath)) {
                throw new ConnectionError(
                        String.format("WebService Connection settings not found. [path=%s]", zkPath));
            }
            settings = (AwsS3Settings) reader.settings();
            settings.validate();
            if (settings.getAuthHandler() != null) {
                auth = settings.getAuthHandler().getDeclaredConstructor()
                        .newInstance();
                auth.init(settings.getAuthSettings(), env.keyStore());
            }

            return setup(settings, env);
        } catch (Exception ex) {
            state.error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings, @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof AwsS3Settings);
        this.settings = (AwsS3Settings) settings;
        this.env = env;
        state.setState(EConnectionState.Initialized);
        return this;
    }

    public S3Client client() throws ConnectionError {
        Preconditions.checkState(state.isConnected());
        synchronized (this) {
            try {
                long delta = System.currentTimeMillis() - connectedTimestamp;
                if (delta > settings.getResetTimeout().normalized()) {
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
    public Connection connect() throws ConnectionError {
        if (!state.isConnected()) {
            Preconditions.checkState(state.getState() == EConnectionState.Initialized);
            Preconditions.checkNotNull(settings);
            try {
                reconnect();
                state.setState(EConnectionState.Connected);
            } catch (Exception ex) {
                state.error(ex);
                DefaultLogger.stacktrace(ex);
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    private void reconnect() throws Exception {
        Region region = Region.of(settings.getRegion());
        S3ClientBuilder builder = S3Client.builder()
                .region(region);
        if (!Strings.isNullOrEmpty(settings.getEndpoint())) {
            builder.endpointOverride(new URI(settings.getEndpoint()));
        }
        if (auth != null) {
            builder.credentialsProvider(auth.credentials());
        }
        client = builder.build();
        connectedTimestamp = System.currentTimeMillis();
    }

    @Override
    public Throwable error() {
        if (state.hasError()) {
            return state.getError();
        }
        return null;
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
        return AwsS3Settings.__CONFIG_PATH;
    }

    @Override
    public ConnectionSettings settings() {
        return settings;
    }

    @Override
    public EConnectionType type() {
        return EConnectionType.s3;
    }

    @Override
    public void close() throws IOException {
        if (state.isConnected()) {
            state.setState(EConnectionState.Closed);
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
