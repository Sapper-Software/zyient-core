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

package io.zyient.base.core.connections.hadoop;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionConfig;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.hadoop.HdfsConnectionSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.io.IOException;
import java.net.URI;

@Getter
@Accessors(fluent = true)
public class HdfsConnection implements Connection {
    private static final String HDFS_PARAM_DEFAULT_FS = "fs.defaultFS";
    private static final String HDFS_PARAM_DFS_IMPLEMENTATION = "fs.hdfs.impl";

    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    private HdfsConfig config;
    protected Configuration hdfsConfig = null;
    protected FileSystem fileSystem;
    protected HdfsAdmin adminClient;
    protected DFSClient dfsClient;

    protected HdfsConnectionSettings.HdfsBaseSettings settings;

    /**
     * @return
     */
    @Override
    public String name() {
        return settings().getName();
    }

    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();
                config = new HdfsConfig(xmlConfig, HdfsConnectionSettings.HdfsSettings.class, HdfsConnection.class);
                config.read();
                settings = (HdfsConnectionSettings.HdfsBaseSettings) config.settings();
                setupHadoopConfig();

                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    private void setupHadoopConfig() throws Exception {
        Preconditions.checkState(settings instanceof HdfsConnectionSettings.HdfsSettings);
        hdfsConfig = new Configuration();
        hdfsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getCanonicalName());
        hdfsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getCanonicalName());
        hdfsConfig.set(HDFS_PARAM_DEFAULT_FS, ((HdfsConnectionSettings.HdfsSettings) settings).getPrimaryNameNodeUri());
        hdfsConfig.set(HDFS_PARAM_DFS_IMPLEMENTATION, DistributedFileSystem.class.getName());
        if (settings.isSecurityEnabled()) {
            enableSecurity(hdfsConfig);
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear();

                CuratorFramework client = connection.client();
                String zkPath = new PathUtils.ZkPathBuilder(path)
                        .withPath(HdfsConfig.__CONFIG_PATH)
                        .build();
                ZkConfigReader reader = new ZkConfigReader(client, HdfsConnectionSettings.HdfsSettings.class);
                if (!reader.read(zkPath)) {
                    throw new ConnectionError(
                            String.format("HDFS Connection settings not found. [path=%s]", zkPath));
                }
                settings = (HdfsConnectionSettings.HdfsBaseSettings) reader.settings();
                settings.validate();

                setupHadoopConfig();

                state.setState(EConnectionState.Initialized);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof HdfsConnectionSettings.HdfsSettings);
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                this.settings = (HdfsConnectionSettings.HdfsBaseSettings) settings;
                setupHadoopConfig();
                state.clear();
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    protected void enableSecurity(Configuration conf) throws Exception {
        ConfigReader reader = new ConfigReader(config.config(),
                HdfsConnectionSettings.HdfsSecuritySettings.__CONFIG_PATH,
                HdfsConnectionSettings.HdfsSecuritySettings.class);
        reader.read();
        HdfsConnectionSettings.HdfsSecuritySettings settings
                = (HdfsConnectionSettings.HdfsSecuritySettings) reader.settings();
        settings.setup(conf);
        this.settings.setSecuritySettings(settings);
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(settings instanceof HdfsConnectionSettings.HdfsSettings);
        synchronized (state) {
            if (!state.isConnected()
                    && (state.getState() == EConnectionState.Initialized
                    || state.getState() == EConnectionState.Closed)) {
                state.clear();
                try {
                    fileSystem = FileSystem.get(hdfsConfig);
                    if (settings.isAdminEnabled()) {
                        adminClient = new HdfsAdmin(URI.create(
                                ((HdfsConnectionSettings.HdfsSettings) settings).getPrimaryNameNodeUri()), hdfsConfig);
                    }
                    if (settings.getParameters() != null && !settings.getParameters().isEmpty()) {
                        for (String key : settings.getParameters().keySet()) {
                            hdfsConfig.set(key, settings.getParameters().get(key));
                        }
                    }
                    dfsClient = new DFSClient(URI.create(
                            ((HdfsConnectionSettings.HdfsSettings) settings).getPrimaryNameNodeUri()), hdfsConfig);
                    state.setState(EConnectionState.Connected);
                } catch (Throwable t) {
                    state.error(t);
                    throw new ConnectionError("Error opening HDFS connection.", t);
                }
            }
        }
        return this;
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return state.getError();
    }

    /**
     * @return
     */
    @Override
    public EConnectionState connectionState() {
        return state.getState();
    }

    /**
     * @return
     */
    @Override
    public boolean isConnected() {
        return state.isConnected();
    }

    @Override
    public String path() {
        return HdfsConfig.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return settings.getType();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.setState(EConnectionState.Closed);
            }
            try {
                if (fileSystem != null) {
                    fileSystem.close();
                    fileSystem = null;
                }
                if (adminClient != null) {
                    adminClient = null;
                }
            } catch (Exception ex) {
                state.error(ex);
                throw new IOException("Error closing HDFS connection.", ex);
            }
        }
    }

    public final FileSystem get() throws ConnectionError {
        if (!state.isConnected()) {
            throw new ConnectionError(String.format("HDFS Connection not available. [state=%s]", state.getState().name()));
        }
        return fileSystem;
    }

    @Getter
    @Accessors(fluent = true)
    public static class HdfsConfig extends ConnectionConfig {

        private static final String __CONFIG_PATH = "hdfs";

        public HdfsConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                          @NonNull Class<? extends HdfsConnectionSettings.HdfsBaseSettings> type,
                          @NonNull Class<? extends HdfsConnection> connectionClass) {
            super(config, __CONFIG_PATH, type, connectionClass);
        }

        public HdfsConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                          @NonNull String path,
                          @NonNull Class<? extends HdfsConnectionSettings.HdfsBaseSettings> type,
                          @NonNull Class<? extends HdfsConnection> connectionClass) {
            super(config, path, type, connectionClass);
        }
    }
}
