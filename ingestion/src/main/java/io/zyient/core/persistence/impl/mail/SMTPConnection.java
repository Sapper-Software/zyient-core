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

package io.zyient.core.persistence.impl.mail;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.keystore.KeyStore;
import io.zyient.core.persistence.impl.settings.mail.SMTPConnectionSettings;
import jakarta.mail.Authenticator;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public class SMTPConnection extends AbstractMailConnection<Session> {
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Session sendSession;

    protected SMTPConnection() {
        super(SMTPConnectionSettings.class, SMTPConnectionSettings.__CONFIG_PATH);
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
            if (sendSession != null) {
                sendSession = null;
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Session connection() {
        Preconditions.checkState(state.isConnected());
        try {
            return sendSession;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    @Override
    public void close(@Nonnull Session session) {

    }

    @Override
    protected void postSetup() throws Exception {

    }

    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(state.getState() == EConnectionState.Initialized);
        try {
            SMTPConnectionSettings settings = (SMTPConnectionSettings) settings();
            // creating Session instance referenced to
            // Authenticator object to pass in
            // Session.getInstance argument
            if (settings.isUseCredentials()) {
                Properties props = settings.get();
                KeyStore keyStore = env.keyStore();
                String password = keyStore.read(settings.getPasskey());
                if (Strings.isNullOrEmpty(password)) {
                    throw new Exception(String.format("Failed to read password. [passkey=%s]", settings.getPasskey()));
                }
                sendSession = Session.getInstance(props,
                        new Authenticator() {

                            // override the getPasswordAuthentication
                            // method
                            protected PasswordAuthentication
                            getPasswordAuthentication() {
                                return new PasswordAuthentication(settings.getUsername(),
                                        password);
                            }
                        });
            } else {
                Properties propsSMTP = new Properties();
                propsSMTP.setProperty(SMTPConnectionSettings.CONFIG_SMTP_HOST_PARAM, settings.getMailServer());
                sendSession = Session.getInstance(propsSMTP);
            }
            state.setState(EConnectionState.Connected);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }
}
