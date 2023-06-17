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

package ai.sapper.cdc.core;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.WebServiceConnection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.JerseyWebTarget;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class WebServiceClient {
    private WebServiceConnection connection;
    private WebServiceClientConfig config;
    private WebServiceClientSettings settings;

    public WebServiceClient() {
    }

    public WebServiceClient(@NonNull WebServiceConnection connection) {
        this.connection = connection;
    }

    public WebServiceClient init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                 @NonNull String configPath,
                                 @NonNull ConnectionManager manager) throws ConfigurationException {
        try {
            config = new WebServiceClientConfig(xmlConfig, configPath);
            config.read();
            settings = (WebServiceClientSettings) config.settings();
            connection = manager.getConnection(settings.connection, WebServiceConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("Connection not found. [name=%s][type=%s]",
                                settings.connection, WebServiceConnection.class.getCanonicalName()));
            }

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public <T> T get(@NonNull String service,
                     @NonNull Class<T> type,
                     List<String> params,
                     String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        String path = settings.pathMap.get(service);
        if (Strings.isNullOrEmpty(path)) {
            throw new ConnectionError(String.format("No service registered with name. [name=%s]", service));
        }
        return getUrl(path, type, params, mediaType);
    }

    public <T> T getUrl(@NonNull String path,
                        @NonNull Class<T> type,
                        Map<String, String> query,
                        String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        JerseyWebTarget target = connection.connect(path);
        if (query != null && !query.isEmpty()) {
            for (String q : query.keySet()) {
                target = target.queryParam(q, query.get(q));
            }
        }

        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        return getUrl(target, type, mediaType);
    }

    public <T> T getUrl(@NonNull String path,
                        @NonNull Class<T> type,
                        List<String> params,
                        String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        JerseyWebTarget target = connection.connect(path);
        if (params != null && !params.isEmpty()) {
            for (String param : params) {
                target = target.path(param);
            }
        }

        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        return getUrl(target, type, mediaType);
    }

    private <T> T getUrl(JerseyWebTarget target, Class<T> type, String mediaType) throws ConnectionError {
        Invocation.Builder builder = target.request(mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                Response response = builder.get();
                if (response != null) {
                    if (response.getStatus() != HttpStatus.SC_OK) {
                        handle = false;
                        throw new ConnectionError(String.format("Service returned status = [%d]", response.getStatus()));
                    }
                    return response.readEntity(type);
                }
                throw new ConnectionError(
                        String.format("Service response was null. [service=%s]", target.getUri().toString()));
            } catch (Throwable t) {
                if (handle && count < settings().retryCount) {
                    count++;
                    DefaultLogger.error(
                            String.format("Error calling web service. [tries=%d][service=%s]",
                                    count, target.getUri().toString()), t);
                } else {
                    throw t;
                }
            }
        }
    }

    public <R, T> T post(@NonNull String service,
                         @NonNull Class<T> type,
                         @NonNull R request,
                         List<String> params,
                         String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        String path = settings.pathMap.get(service);
        if (Strings.isNullOrEmpty(path)) {
            throw new ConnectionError(String.format("No service registered with name. [name=%s]", service));
        }
        JerseyWebTarget target = connection.connect(path);
        if (params != null && !params.isEmpty()) {
            for (String param : params) {
                target = target.path(param);
            }
        }
        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        Invocation.Builder builder = target.request(mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                try (Response response = builder.post(Entity.entity(request, mediaType))) {
                    if (response != null) {
                        if (response.getStatus() != HttpStatus.SC_OK) {
                            handle = false;
                            throw new ConnectionError(String.format("Service returned status = [%d]", response.getStatus()));
                        }
                        return response.readEntity(type);
                    }
                    throw new ConnectionError(
                            String.format("Service response was null. [service=%s]", target.getUri().toString()));
                }
            } catch (Throwable t) {
                if (handle && count < settings().retryCount) {
                    count++;
                    DefaultLogger.error(
                            String.format("Error calling web service. [tries=%d][service=%s]",
                                    count, target.getUri().toString()), t);
                } else {
                    throw t;
                }
            }
        }
    }

    public <R, T> T put(@NonNull String service,
                        @NonNull Class<T> type,
                        @NonNull R request,
                        List<String> params,
                        String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        String path = settings.pathMap.get(service);
        if (Strings.isNullOrEmpty(path)) {
            throw new ConnectionError(String.format("No service registered with name. [name=%s]", service));
        }
        JerseyWebTarget target = connection.connect(path);
        if (params != null && !params.isEmpty()) {
            for (String param : params) {
                target = target.path(param);
            }
        }
        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        Invocation.Builder builder = target.request(mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                try (Response response = builder.put(Entity.entity(request, mediaType))) {
                    if (response != null) {
                        if (response.getStatus() != HttpStatus.SC_OK) {
                            handle = false;
                            throw new ConnectionError(String.format("Service returned status = [%d]", response.getStatus()));
                        }
                        return response.readEntity(type);
                    }
                    throw new ConnectionError(
                            String.format("Service response was null. [service=%s]", target.getUri().toString()));
                }
            } catch (Throwable t) {
                if (handle && count < settings().retryCount) {
                    count++;
                    DefaultLogger.error(
                            String.format("Error calling web service. [tries=%d][service=%s]",
                                    count, target.getUri().toString()), t);
                } else {
                    throw t;
                }
            }
        }
    }

    public <T> T delete(@NonNull String service,
                        @NonNull Class<T> type,
                        List<String> params,
                        String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        String path = settings.pathMap.get(service);
        if (Strings.isNullOrEmpty(path)) {
            throw new ConnectionError(String.format("No service registered with name. [name=%s]", service));
        }
        JerseyWebTarget target = connection.connect(path);
        if (params != null && !params.isEmpty()) {
            for (String param : params) {
                target = target.path(param);
            }
        }
        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        Invocation.Builder builder = target.request(mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                try (Response response = builder.delete()) {
                    if (response != null) {
                        if (response.getStatus() != HttpStatus.SC_OK) {
                            handle = false;
                            throw new ConnectionError(String.format("Service returned status = [%d]", response.getStatus()));
                        }
                        return response.readEntity(type);
                    }
                    throw new ConnectionError(
                            String.format("Service response was null. [service=%s]", target.getUri().toString()));
                }
            } catch (Throwable t) {
                if (handle && count < settings().retryCount) {
                    count++;
                    DefaultLogger.error(
                            String.format("Error calling web service. [tries=%d][service=%s]",
                                    count, target.getUri().toString()), t);
                } else {
                    throw t;
                }
            }
        }
    }

    @Getter
    @Setter
    public static class WebServiceClientSettings extends Settings {
        public static class Constants {
            public static final String CONFIG_NAME = "name";
            public static final String CONFIG_CONNECTION = "connection";
            public static final String CONFIG_PATH_MAP = "paths";
            public static final String CONFIG_RETRIES = "retryCount";
        }

        @Config(name = Constants.CONFIG_NAME)
        private String name;
        @Config(name = Constants.CONFIG_CONNECTION)
        private String connection;
        @Config(name = Constants.CONFIG_PATH_MAP)
        private Map<String, String> pathMap;
        @Config(name = Constants.CONFIG_RETRIES, required = false, type = Integer.class)
        private int retryCount = 0;
    }

    public static class WebServiceClientConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "client";


        public WebServiceClientConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, String.format("%s.%s", path, __CONFIG_PATH), WebServiceClientSettings.class);
        }
    }
}
