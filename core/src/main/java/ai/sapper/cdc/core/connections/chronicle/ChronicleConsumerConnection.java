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

package ai.sapper.cdc.core.connections.chronicle;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.EMessageClientMode;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settings.ConnectionSettings;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class ChronicleConsumerConnection extends ChronicleConnection {
    private final Map<String, ExcerptTailer> tailers = new HashMap<>();

    @Override
    public boolean canSend() {
        return false;
    }

    @Override
    public boolean canReceive() {
        return true;
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            super.init(xmlConfig, env);
            try {
                setup();
                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening Chronicle connection.", t);
            }
        }
        return this;
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            super.init(name, connection, path, env);
            try {
                setup();
                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening Chronicle connection.", t);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            super.setup(settings, env);
            try {
                setup();
                state.setState(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening Chronicle connection.", t);
            }
            return this;
        }
    }

    private void setup() throws Exception {
        if (settings().getMode() != EMessageClientMode.Consumer) {
            throw new ConfigurationException("Connection not initialized in Consumer mode.");
        }
        settings().setConnectionClass(getClass());
    }

    @Override
    public Connection connect() throws ConnectionError {
        synchronized (state) {
            try {
                Preconditions.checkState(connectionState() == EConnectionState.Initialized);
                setupQueue();
                state.setState(EConnectionState.Connected);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    public ExcerptTailer get(@NonNull String name) {
        Preconditions.checkState(isConnected());
        synchronized (tailers) {
            if (tailers.containsKey(name)) {
                return tailers.get(name);
            }
            ExcerptTailer tailer = queue.createTailer(name);
            tailers.put(name, tailer);
            return tailer;
        }
    }

    public void release(@NonNull String name) {
        Preconditions.checkState(isConnected());
        synchronized (tailers) {
            if (tailers.containsKey(name)) {
                tailers.get(name).close();
                tailers.remove(name);
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (!tailers.isEmpty()) {
                for (String name : tailers.keySet()) {
                    tailers.get(name).close();
                }
                tailers.clear();
            }
            super.close();
        }
    }
}
