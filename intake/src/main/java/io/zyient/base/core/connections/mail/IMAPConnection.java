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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.cache.ThreadCache;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.settings.IMAPConnectionSettings;
import jakarta.mail.Session;
import jakarta.mail.Store;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class IMAPConnection extends AbstractMailConnection<Store> {


    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private ThreadCache<Store> threadCache = new ThreadCache<>();

    protected IMAPConnection() {
        super(IMAPConnectionSettings.class, IMAPConnectionSettings.__CONFIG_PATH);
    }

    public String getEmailId() {
        Preconditions.checkState(settings instanceof IMAPConnectionSettings);
        return ((IMAPConnectionSettings) settings).getEmailId();
    }

    public String getMailServer() {
        Preconditions.checkState(settings instanceof IMAPConnectionSettings);
        return ((IMAPConnectionSettings) settings).getMailServer();
    }

    public int getPort() {
        Preconditions.checkState(settings instanceof IMAPConnectionSettings);
        return ((IMAPConnectionSettings) settings).getPort();
    }

    public String getUsername() {
        Preconditions.checkState(settings instanceof IMAPConnectionSettings);
        return ((IMAPConnectionSettings) settings).getUsername();
    }

    public void open() throws Exception {
        Preconditions.checkState(state.isConnected());
        IMAPConnectionSettings settings = (IMAPConnectionSettings) settings();
        Session readSession = Session.getInstance(settings.get());
        Store store = readSession.getStore();
        DefaultLogger.warn(String.format(" [store=%s]", store));
        String password = env.keyStore().read(settings.getPasskey());
        if (Strings.isNullOrEmpty(password)) {
            throw new Exception(String.format("Failed to read password. [key=%s]", settings.getPasskey()));
        }
        store.connect(settings.getUsername(), password);
        threadCache.put(store);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        try {
            if (state.isConnected()) {
                DefaultLogger.info(String.format("Closing Data Source : [name=%s]", name()));
                state.setState(EConnectionState.Closed);
            } else if (state.hasError()) {
                DefaultLogger.warn(String.format("Data Source in error state. " +
                        "[name=%s][error=%s]", name(), state.getError().getLocalizedMessage()));
            }
            threadCache.close(store -> {
                if (store != null && store.isConnected()) store.close();
            });
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Store connection() {
        Preconditions.checkState(state.isConnected());
        try {
            if (!threadCache.contains()) {
                open();
            }
            IMAPConnectionSettings settings = (IMAPConnectionSettings) settings();
            Store store = threadCache.get();
            if (!store.isConnected()) {
                String password = env.keyStore().read(settings.getPasskey());
                if (Strings.isNullOrEmpty(password)) {
                    throw new Exception(String.format("Failed to read password. [key=%s]", settings.getPasskey()));
                }
                store.connect(settings.getUsername(), password);
            }
            return store;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    @Override
    public void close(@Nonnull Store store) throws ConnectionError {
        close(Thread.currentThread().getId());
    }

    public void close(long threadId) throws ConnectionError {
        try {
            if (threadCache.contains()) {
                Store store = threadCache.get();
                if (store != null && store.isConnected()) store.close();
                threadCache.remove();
            }
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }

    @Override
    protected void postSetup() throws Exception {

    }

    @Override
    public Connection connect() throws ConnectionError {
        return null;
    }
}
