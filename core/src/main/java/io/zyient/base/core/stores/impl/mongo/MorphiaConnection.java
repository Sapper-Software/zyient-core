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

package io.zyient.base.core.stores.impl.mongo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.transactions.MorphiaSession;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.db.DbConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.keystore.KeyStore;
import io.zyient.base.core.stores.AbstractConnection;
import io.zyient.base.core.stores.impl.settings.mongo.MorphiaConnectionSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class MorphiaConnection extends AbstractConnection<MorphiaSession> {
    private MorphiaConnectionSettings settings;
    protected ConnectionManager connectionManager;
    private BaseEnv<?> env;
    private Datastore datastore;
    private MongoClient client;

    public MorphiaConnection() {
        super(EConnectionType.db, MorphiaConnectionSettings.class);
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings, @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state()) {
            try {
                if (state().isConnected()) {
                    close();
                }
                state().clear();
                this.settings = (MorphiaConnectionSettings) settings;
                this.connectionManager = env.connectionManager();
                state().setState(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                state().error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection connect() throws ConnectionError {
        try {
            if (!state().isConnected()) {
                state().check(EConnectionState.Initialized);
                datastore = createConnection();
                state().setState(EConnectionState.Connected);
            }
            return this;
        } catch (Exception ex) {
            state().error(ex);
            throw new ConnectionError(ex);
        }
    }

    public Datastore getConnection() throws ConnectionError {
        state().check(EConnectionState.Connected);
        return datastore;
    }

    private Datastore createConnection() throws Exception {
        if (client == null) {
            KeyStore keyStore = connectionManager().keyStore();
            Preconditions.checkNotNull(keyStore);
            String url = createConnectionUrl(keyStore);
            client = MongoClients.create(url);
        }
        return Morphia.createDatastore(client, settings.getDb());
    }

    protected String createConnectionUrl(KeyStore keyStore) throws Exception {
        String url = "mongodb://";
        StringBuilder builder = new StringBuilder(url);
        builder.append(settings.getUser());
        String passwd = keyStore.read(settings.getPassword());
        if (Strings.isNullOrEmpty(passwd)) {
            throw new Exception(String.format("Failed to get encrypted key. [key=%s]", settings.getPassword()));
        }
        builder.append(":")
                .append(passwd);
        builder.append("@")
                .append(settings.getHost())
                .append(":")
                .append(settings.getPort());
        builder.append("/")
                .append(settings.getAuthDb());
        builder.append("?")
                .append(DbConnection.Constants.DB_KEY_POOL_SIZE)
                .append(settings.getPoolSize());
        if (settings.getParameters() != null && !settings.getParameters().isEmpty()) {
            for (String param : settings.getParameters().keySet()) {
                builder.append("&")
                        .append(param)
                        .append("=")
                        .append(settings.getParameters().get(param));
            }
        }
        return builder.toString();
    }

    @Override
    public boolean hasTransactionSupport() {
        return true;
    }

    @Override
    public void close(@NonNull MorphiaSession session) throws ConnectionError {
        if (session.hasActiveTransaction()) {
            throw new ConnectionError("Session has active transactions...");
        }
        session.close();
    }

    public MorphiaSession getSession() {
        Preconditions.checkState(state().isConnected());
        return datastore.startSession();
    }

    @Override
    public void close() throws IOException {
        synchronized (state()) {
            if (state().isConnected())
                state().setState(EConnectionState.Closed);
            if (datastore != null) {
                client.close();
                client = null;
                datastore = null;
            }
        }
    }
}
