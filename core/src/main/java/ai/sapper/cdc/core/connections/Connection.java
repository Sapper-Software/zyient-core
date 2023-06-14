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
