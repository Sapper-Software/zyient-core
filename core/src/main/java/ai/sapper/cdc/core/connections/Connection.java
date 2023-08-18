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

package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.settings.ConnectionSettings;
import ai.sapper.cdc.core.connections.settings.EConnectionType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;

public interface Connection extends Closeable {
    enum EConnectionState {
        Unknown, Initialized, Connected, Closed, Error
    }

    class ConnectionState extends AbstractState<EConnectionState> {

        public ConnectionState() {
            super(EConnectionState.Error, EConnectionState.Unknown);
            setState(EConnectionState.Unknown);
        }

        public boolean isConnected() {
            return (getState() == EConnectionState.Connected);
        }

        public void check(@NonNull EConnectionState state) throws ConnectionError {
            if (state != getState()) {
                throw new ConnectionError(
                        String.format("Invalid connection state: [expected=%s][state=%s]",
                                state.name(), getState().name()));
            }
        }
    }

    String name();

    Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                    @NonNull BaseEnv<?> env) throws ConnectionError;

    Connection init(@NonNull String name,
                    @NonNull ZookeeperConnection connection,
                    @NonNull String path,
                    @NonNull BaseEnv<?> env) throws ConnectionError;

    Connection setup(@NonNull ConnectionSettings settings,
                     @NonNull BaseEnv<?> env) throws ConnectionError;

    Connection connect() throws ConnectionError;

    Throwable error();

    EConnectionState connectionState();

    @JsonIgnore
    boolean isConnected();

    String path();

    ConnectionSettings settings();

    EConnectionType type();
}
