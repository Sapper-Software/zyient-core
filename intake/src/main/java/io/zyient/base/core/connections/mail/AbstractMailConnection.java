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

package io.zyient.base.core.connections.mail;

import io.zyient.base.common.ICloseDelegate;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.MailConnectionSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public abstract class AbstractMailConnection<T> extends AbstractConnection<T> implements ICloseDelegate<T> {
    private final String path;
    @Getter(AccessLevel.NONE)
    protected final Connection.ConnectionState state = new Connection.ConnectionState();
    protected BaseEnv<?> env;
    protected MailConfigReader configReader;

    protected AbstractMailConnection(@NonNull Class<? extends MailConnectionSettings> settingsType,
                                     String path) {
        super(EConnectionType.email, settingsType);
        this.path = path;
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            if (state.isConnected()) return this;
            try {
                super.init(config, env);
                return setup(settings, env);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            if (state.isConnected()) return this;
            try {
                super.init(name, connection, path, env);
                return setup(settings, env);
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            this.env = env;
            postSetup();
            state.setState(Connection.EConnectionState.Initialized);
            return this;
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Throwable error() {
        return state.getError();
    }

    @Override
    public Connection.EConnectionState connectionState() {
        return state.getState();
    }

    @Override
    public boolean isConnected() {
        return state.isConnected();
    }


    @Override
    public EConnectionType type() {
        return EConnectionType.email;
    }

    public abstract T connection() throws ConnectionError;

    public abstract boolean hasTransactionSupport();

    protected abstract void postSetup() throws Exception;

    public static class MailConfigReader extends ConfigReader {
        public static final String __CONFIG_PATH = "mail";

        public MailConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                @NonNull String path,
                                @NonNull Class<? extends MailConnectionSettings> type) {
            super(config, path, type);
        }

        public MailConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                @NonNull Class<? extends MailConnectionSettings> type) {
            super(config, __CONFIG_PATH, type);
        }

    }
}
