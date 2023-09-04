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

package ai.sapper.cdc.core.connections.mail;

import ai.sapper.cdc.common.cache.ThreadCache;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.settings.ExchangeConnectionSettings;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import microsoft.exchange.webservices.data.core.ExchangeService;
import microsoft.exchange.webservices.data.core.enumeration.misc.ConnectingIdType;
import microsoft.exchange.webservices.data.core.enumeration.misc.ExchangeVersion;
import microsoft.exchange.webservices.data.credential.ExchangeCredentials;
import microsoft.exchange.webservices.data.credential.WebCredentials;
import microsoft.exchange.webservices.data.misc.ImpersonatedUserId;

import javax.annotation.Nonnull;
import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

@Getter
@Setter
@Accessors(fluent = true)
public class ExchangeConnection extends AbstractMailConnection<ExchangeService> {
    private ExchangeService service;
    @Getter(AccessLevel.NONE)
    private ThreadCache<ExchangeService> threadCache = new ThreadCache<>();
    @Getter(AccessLevel.NONE)
    private KeyStore keyStore;

    public ExchangeConnection() {
        super(ExchangeConnectionSettings.class, ExchangeConnectionSettings.__CONFIG_PATH);
    }

    public void open() throws Exception {
        Preconditions.checkState(state.isConnected());
        ExchangeConnectionSettings settings = (ExchangeConnectionSettings) settings();
        ExchangeService service = new ExchangeService(ExchangeVersion.Exchange2010_SP2);
        ExchangeCredentials credentials = null;
        String password = keyStore.read(settings.getPasskey());
        if (Strings.isNullOrEmpty(password)) {
            throw new Exception(String.format("Failed to read password. [key=%s]", settings.getPasskey()));
        }
        if (Strings.isNullOrEmpty(settings.getTenantId())) {
            if (Strings.isNullOrEmpty(settings.getDomain())) {
                credentials = new WebCredentials(settings.getUsername(), password);
            } else {
                credentials = new WebCredentials(settings.getUsername(), password, settings.getDomain());
            }

            service.setCredentials(credentials);
            if (!Strings.isNullOrEmpty(settings.getProxyUser())) {
                service.setImpersonatedUserId(
                        new ImpersonatedUserId(ConnectingIdType.PrincipalName, settings.getProxyUser()));
            }
        } else {
            String accessToken = getAccessToken(settings.getUsername(), password, settings.getTenantId());
            DefaultLogger.info(String.format("accessToken=%s, emailId=%s", accessToken, settings.getEmailId()));

            service.getHttpHeaders().put("Authorization", "Bearer " + accessToken);
            service.getHttpHeaders().put("X-AnchorMailbox", settings.getEmailId());
            service.setImpersonatedUserId(new ImpersonatedUserId(ConnectingIdType.PrincipalName, settings.getEmailId()));
        }
        service.setUrl(new URI(settings.getMailServer()));
        if (settings.getRequestTimeout() > 0) {
            service.setTimeout(((ExchangeConnectionSettings) settings()).getRequestTimeout());
        }
        if (DefaultLogger.isTraceEnabled()) {
            service.setTraceEnabled(true);
        }
        threadCache.put(service);
    }

    @Override
    public ExchangeService connection() throws ConnectionError {
        Preconditions.checkState(state.isConnected());
        try {
            if (!threadCache.contains()) {
                open();
            }
            return threadCache.get();
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    @Override
    public void close(@Nonnull ExchangeService exchangeService) throws ConnectionError {
        exchangeService.close();
    }

    @Override
    public void close() throws IOException {
        if (threadCache.contains()) {
            threadCache.get().close();
            threadCache.remove();
        }
    }

    public boolean useDelegate() {
        Preconditions.checkState(state.getState() == EConnectionState.Initialized || state.isConnected());
        ExchangeConnectionSettings settings = (ExchangeConnectionSettings) settings();
        return !Strings.isNullOrEmpty(settings.getDelegateUser());
    }

    public static String getAccessToken(String username,
                                        String password,
                                        String tenantId) throws Exception {
        if (Strings.isNullOrEmpty(System.getenv("OAUTH_ENDPOINT")) || Strings.isNullOrEmpty(System.getenv("OUTLOOK_RESOURCE"))) {
            throw new Exception(
                    String.format("ExchangeConnection : Exception is OAUTH_ENDPOINT or OUTLOOK_RESOURCE information missing [OAUTH_ENDPOINT=%s] [OUTLOOK_RESOURCE=%s]",
                            System.getenv("OAUTH_ENDPOINT"), System.getenv("OUTLOOK_RESOURCE")));
        }
        String endpoint = String.format(System.getenv("OAUTH_ENDPOINT"), tenantId);
        String postBody = String.format("grant_type=client_credentials&client_id=%s&client_secret=%s&resource=%s",
                username, password, System.getenv("OUTLOOK_RESOURCE"));
        HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
        conn.setRequestMethod(String.valueOf(HttpMethod.POST));
        conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setDoOutput(true);
        conn.getOutputStream().write(postBody.getBytes());
        conn.connect();

        JsonFactory factory = new JsonFactory();
        JsonParser parser = factory.createParser(conn.getInputStream());
        String accessToken = null;

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String name = parser.getCurrentName();
            if ("access_token".equals(name)) {
                parser.nextToken();
                accessToken = parser.getText();
            }
        }
        return accessToken;
    }

    @Override
    protected void postSetup() throws Exception {

    }

    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(state.getState() == EConnectionState.Initialized);
        try {
            keyStore = env.keyStore();
            if (keyStore == null) {
                throw new ConnectionError(String.format("[%s] KeyStore not defined.", env.name()));
            }
            open();
            state.setState(EConnectionState.Connected);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }


    @Override
    public String path() {
        return ExchangeConnectionSettings.__CONFIG_PATH;
    }
}
