/*
 *  Copyright (2019) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package ai.sapper.cdc.core.stores;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settings.EConnectionType;
import ai.sapper.cdc.core.model.IEntity;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashSet;
import java.util.Set;


@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractConnection<T> implements Connection {
    @Setter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();
    protected AbstractConnectionSettings settings;
    private final Class<? extends AbstractConnectionSettings> settingsType;
    private final EConnectionType type;

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
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        return null;
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        return null;
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
}
