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

package io.zyient.base.core.connections.ws;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.ConnectionManager;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

import java.io.File;
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
                try (Response response = builder.get()) {
                    if (response != null) {
                        int rc = response.getStatus();
                        if (rc != HttpStatus.SC_OK) {
                            if (rc >= 200 && rc < 300) {
                                handle = false;
                            }
                            throw new ConnectionError(String.format("Service returned status = [%d][url=%s]",
                                    response.getStatus(), target.getUri().toString()));
                        }
                        if (response.hasEntity())
                            return response.readEntity(type);
                    }
                    handle = false;
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

    public <R, T> T multipart(@NonNull String service,
                              @NonNull Class<T> type,
                              @NonNull R request,
                              List<String> params,
                              @NonNull String fileVar,
                              @NonNull String entityVar,
                              @NonNull File file) throws ConnectionError {
        Preconditions.checkNotNull(this.connection);
        WebServiceConnection connection = this.connection.multipartConnection();
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
        if (!file.exists()) {
            throw new ConnectionError(String.format("Invalid source file. [path=%s]", file.getAbsolutePath()));
        }
        try {
            FileDataBodyPart filePart = new FileDataBodyPart(fileVar, file);
            // UPDATE: just tested again, and the below code is not needed.
            // It's redundant. Using the FileDataBodyPart already sets the
            // Content-Disposition information
            filePart.setContentDisposition(
                    FormDataContentDisposition.name(fileVar)
                            .fileName(file.getName()).build());
            String json = JSONUtils.asString(request, request.getClass());
            try (FormDataMultiPart multipartEntity = (FormDataMultiPart) new FormDataMultiPart()
                    .field(entityVar, json, MediaType.APPLICATION_JSON_TYPE)
                    .bodyPart(filePart)) {
                int count = 0;
                boolean handle = true;
                while (true) {
                    try (Response response = target.request().post(
                            Entity.entity(multipartEntity, multipartEntity.getMediaType()))) {
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
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }

    public <R, T> T post(@NonNull String service,
                         @NonNull Class<T> type,
                         @NonNull R request,
                         List<String> params,
                         String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(this.connection);
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
        Invocation.Builder builder = connection.build(target, mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                try (Response response = builder.post(Entity.entity(request, mediaType))) {
                    if (response != null) {
                        int rc = response.getStatus();
                        if (rc != HttpStatus.SC_OK) {
                            if (rc >= 200 && rc < 300) {
                                handle = false;
                            }
                            throw new ConnectionError(String.format("Service returned status = [%d][url=%s]",
                                    response.getStatus(), target.getUri().toString()));
                        }
                        if (response.hasEntity())
                            return response.readEntity(type);
                    }
                    handle = false;
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
                        int rc = response.getStatus();
                        if (rc != HttpStatus.SC_OK) {
                            if (rc >= 200 && rc < 300) {
                                handle = false;
                            }
                            throw new ConnectionError(String.format("Service returned status = [%d][url=%s]",
                                    response.getStatus(), target.getUri().toString()));
                        }
                        if (response.hasEntity())
                            return response.readEntity(type);
                    }
                    handle = false;
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
                        int rc = response.getStatus();
                        if (rc != HttpStatus.SC_OK) {
                            if (rc >= 200 && rc < 300) {
                                handle = false;
                            }
                            throw new ConnectionError(String.format("Service returned status = [%d][url=%s]",
                                    response.getStatus(), target.getUri().toString()));
                        }
                        if (response.hasEntity())
                            return response.readEntity(type);
                    }
                    handle = false;
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
        @Config(name = Constants.CONFIG_PATH_MAP, type = Map.class)
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
