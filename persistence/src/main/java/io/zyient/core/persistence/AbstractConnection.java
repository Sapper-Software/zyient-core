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

package io.zyient.core.persistence;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.EConnectionType;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashSet;
import java.util.Set;


@Getter
@Accessors(fluent = true)
public abstract class AbstractConnection<T> implements Connection {
    @Setter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();
    protected AbstractConnectionSettings settings;
    private final Class<? extends AbstractConnectionSettings> settingsType;
    private final EConnectionType type;
    private BaseEnv<?> env;

    public AbstractConnection(@NonNull EConnectionType type,
                              @NonNull Class<? extends AbstractConnectionSettings> settingsType) {
        this.type = type;
        this.settingsType = settingsType;
    }

    public void addSupportedType(@NonNull Class<? extends IEntity<?>> type) {
        Preconditions.checkState(state.getState() == EConnectionState.Initialized || state.isConnected());
        Preconditions.checkNotNull(settings);
        Set<Class<?>> types = settings.getSupportedTypes();
        if (types == null) {
            types = new HashSet<>();
            settings.setSupportedTypes(types);
        }
        types.add(type);
    }

    public Set<Class<?>> getSupportedTypes() {
        Preconditions.checkState(state.getState() == EConnectionState.Initialized || state.isConnected());
        Preconditions.checkNotNull(settings);
        return settings.getSupportedTypes();
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            this.env = env;
            ConfigPath cp = settingsType.getAnnotation(ConfigPath.class);
            String zkPath = new PathUtils.ZkPathBuilder(path)
                    .withPath(cp.path())
                    .build();
            CuratorFramework client = connection.client();
            settings = JSONUtils.read(client, zkPath, settingsType);
            if (settings == null) {
                throw new Exception(String.format("Connection settings not found. [path=%s]", zkPath));
            }
            setup(settings, env);
            state.setState(EConnectionState.Initialized);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            this.env = env;
            ConfigPath cp = settingsType.getAnnotation(ConfigPath.class);
            ConfigReader reader = new ConfigReader(config, cp.path(), settingsType);
            reader.read();
            settings = (AbstractConnectionSettings) reader.settings();
            setup(settings, env);
            state.setState(EConnectionState.Initialized);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public String path() {
        ConfigPath cp = settingsType.getAnnotation(ConfigPath.class);
        return cp.path();
    }

    @Override
    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
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
    public EConnectionType type() {
        return type;
    }

    @Override
    public boolean isConnected() {
        return state.isConnected();
    }

    public abstract boolean hasTransactionSupport();

    public abstract void close(@NonNull T connection) throws ConnectionError;
}
