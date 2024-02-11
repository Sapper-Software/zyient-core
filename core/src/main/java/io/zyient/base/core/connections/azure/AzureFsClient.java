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

package io.zyient.base.core.connections.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.azure.auth.AzureStorageAuth;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.aws.AwsS3Settings;
import io.zyient.base.core.keystore.KeyStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.Locale;

@Getter
@Accessors(fluent = true)
public class AzureFsClient implements Connection {
    private final ConnectionState state = new ConnectionState();
    private BlobServiceClient client;
    private AzureFsClientSettings settings;
    private AzureStorageAuth auth;

    public AzureFsClient init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull KeyStore keyStore) throws IOException {
        try {
            ConfigReader config = new ConfigReader(xmlConfig, AzureFsClientSettings.__CONFIG_PATH, AzureFsClientSettings.class);
            config.read();
            settings = (AzureFsClientSettings) config.settings();

            setup(keyStore);
            settings.setAuthSettings(auth.settings());

            state.setState(EConnectionState.Connected);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new IOException(ex);
        }
    }

    public AzureFsClient init(@NonNull AzureFsClientSettings settings,
                              @NonNull KeyStore keyStore) throws IOException {
        Preconditions.checkNotNull(settings.getAuthSettings());
        try {
            this.settings = settings;
            setup(keyStore);

            state.setState(EConnectionState.Connected);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void setup(KeyStore keyStore) throws Exception {
        Class<? extends AzureStorageAuth> ac = (Class<? extends AzureStorageAuth>) Class.forName(settings.getAuthClass());
        auth = ac.getDeclaredConstructor()
                .newInstance()
                .withAccount(settings.getAuthAccount())
                .init(settings.getAuthSettings(), keyStore);

        String endpoint = String.format(Locale.ROOT, settings.getEndpointUrl(), settings.getAuthAccount());
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .endpoint(endpoint);
        client = auth.credentials(builder).buildClient();
    }

    public BlobContainerClient getContainer(@NonNull String container) throws IOException {
        if (client == null) {
            throw new IOException("Storage account not initialized...");
        }
        return client.getBlobContainerClient(container);
    }

    @Override
    public String name() {
        return settings.getName();
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            return init(config, env.keyStore());
        } catch (Exception ex) {
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
            settings = (AzureFsClientSettings) reader.settings();
            return setup(settings, env);
        } catch (Exception ex) {
            state.error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof AzureFsClientSettings);
        try {
            this.settings = (AzureFsClientSettings) settings;
            setup(env.keyStore());
            return this;
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection connect() throws ConnectionError {
        return this;
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
        return AzureFsClientSettings.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return EConnectionType.azureFs;
    }

    @Override
    public void close() throws IOException {
        if (!state.hasError()) {
            state.setState(EConnectionState.Closed);
        }
        client = null;
    }
}
