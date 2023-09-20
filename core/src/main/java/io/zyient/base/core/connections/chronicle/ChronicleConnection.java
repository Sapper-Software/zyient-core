/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.connections.chronicle;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.MessageConnection;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.chronicle.ChronicleSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class ChronicleConnection extends MessageConnection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    private ChronicleConfig config;
    @Getter(AccessLevel.NONE)
    protected ChronicleQueue queue;
    protected BaseEnv<?> env;
    private String messageDir;

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
            config = new ChronicleConfig(xmlConfig);
            config.read();

            settings = (ChronicleSettings) config.settings();
            settings.validate();
            this.env = env;
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection init(@NonNull String name, @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            CuratorFramework client = connection.client();
            String zkPath = new PathUtils.ZkPathBuilder(path)
                    .withPath(ChronicleConfig.__CONFIG_PATH)
                    .build();
            ZkConfigReader reader = new ZkConfigReader(client, ChronicleSettings.class);
            if (!reader.read(zkPath)) {
                throw new ConnectionError(
                        String.format("Chronicle Connection settings not found. [path=%s]", zkPath));
            }
            settings = (ChronicleSettings) reader.settings();
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
        Preconditions.checkArgument(settings instanceof ChronicleSettings);
        try {
            if (state.isConnected()) {
                close();
            }
            state.clear();
            this.settings = (ChronicleSettings) settings;
            this.settings.validate();
            this.env = env;
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    protected void setupQueue() throws Exception {
        Preconditions.checkState(settings instanceof ChronicleSettings);
        messageDir = String.format("%s/%s", ((ChronicleSettings) settings).getBaseDir(), settings.getQueue());
        File dir = new File(messageDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(String.format("Failed to create directory. [path=%s]", dir.getAbsolutePath()));
            }
        }
        queue = ChronicleQueue.singleBuilder(dir)
                .indexSpacing(((ChronicleSettings) settings).getIndexSpacing())
                .rollCycle(((ChronicleSettings) settings).getRollCycle())
                .build();
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
        return ChronicleConfig.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return EConnectionType.chronicle;
    }

    @Override
    public void close() throws IOException {
        if (queue != null) {
            queue.close();
        }
        if (state.isConnected()) {
            state.setState(EConnectionState.Closed);
        }
    }

    public static class ChronicleConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "chronicle";

        public ChronicleConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, ChronicleSettings.class);
        }

        public ChronicleConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                               @NonNull String path,
                               @NonNull Class<? extends ChronicleSettings> type) {
            super(config, path, type);
        }
    }
}
