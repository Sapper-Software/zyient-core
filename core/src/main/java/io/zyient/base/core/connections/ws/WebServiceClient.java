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
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Getter
@Accessors(fluent = true)
public class WebServiceClient {
    private static final int RETRY_INITIAL_INTERVAL = 200;
    private static final int RETRY_MULTIPLIER = 5;

    private WebServiceConnection connection;
    private WebServiceClientConfig config;
    private WebServiceClientSettings settings;
    private IntervalFunction intervalFn;

    private RetryConfig retryConfig;
    private RetryRegistry registry;

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
            if (!connection.isConnected()) {
                connection.connect();
            }
            intervalFn =
                    IntervalFunction.ofExponentialBackoff(RETRY_INITIAL_INTERVAL, RETRY_MULTIPLIER);
            retryConfig = RetryConfig.custom()
                    .maxAttempts(settings.retryCount)
                    .intervalFunction(intervalFn)
                    .failAfterMaxAttempts(true)
                    .retryExceptions(IOException.class, TimeoutException.class)
                    .retryOnResult(response -> ((Response) response).getStatus() < 200 || ((Response) response).getStatus() >= 300)
                    .writableStackTraceEnabled(true)
                    .build();
            registry = RetryRegistry.of(retryConfig);
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
        return getWithPath(path, type, params, mediaType);
    }

    public <T> T get(@NonNull String path,
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
        return getWithPath(target, type, mediaType);
    }

    public <T> T getWithPath(@NonNull String path,
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
        return getWithPath(target, type, mediaType);
    }

    private <T> T getWithPath(JerseyWebTarget target, Class<T> type, String mediaType) throws ConnectionError {
        try {
            if (Strings.isNullOrEmpty(mediaType)) {
                mediaType = MediaType.APPLICATION_JSON;
            }
            Retry retry = registry.retry(target.getUri().toString());
            GetCallable callable = new GetCallable(this, target, mediaType);
            try (Response response = retry.executeCallable(callable)) {
                int rc = response.getStatus();
                if (rc != HttpStatus.SC_OK) {
                    throw new ConnectionError(String.format("Service returned status = [%d][url=%s]",
                            response.getStatus(), target.getUri().toString()));
                }
                if (response.hasEntity())
                    return response.readEntity(type);
                throw new ConnectionError(
                        String.format("Service response entity was null. [service=%s]", target.getUri().toString()));
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConnectionError(ex);
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
        try {
            JerseyWebTarget target = connection.connect(path);
            if (params != null && !params.isEmpty()) {
                for (String param : params) {
                    target = target.path(param);
                }
            }
            if (Strings.isNullOrEmpty(mediaType)) {
                mediaType = MediaType.APPLICATION_JSON;
            }
            Retry retry = registry.retry(target.getUri().toString());
            PostCallable<R> callable = new PostCallable<>(this, target, request, mediaType);
            try (Response response = retry.executeCallable(callable)) {
                int rc = response.getStatus();
                if (rc != HttpStatus.SC_OK) {
                    throw new ConnectionError(String.format("Service returned status = [%d][url=%s]",
                            response.getStatus(), target.getUri().toString()));
                }
                if (response.hasEntity())
                    return response.readEntity(type);
                throw new ConnectionError(
                        String.format("Service response entity was null. [service=%s]", target.getUri().toString()));
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConnectionError(ex);
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
        try {
            JerseyWebTarget target = connection.connect(path);
            if (params != null && !params.isEmpty()) {
                for (String param : params) {
                    target = target.path(param);
                }
            }
            if (Strings.isNullOrEmpty(mediaType)) {
                mediaType = MediaType.APPLICATION_JSON;
            }
            Retry retry = registry.retry(target.getUri().toString());
            PutCallable<R> callable = new PutCallable<>(this, target, request, mediaType);
            try (Response response = retry.executeCallable(callable)) {
                int rc = response.getStatus();
                if (rc != HttpStatus.SC_OK) {
                    throw new ConnectionError(String.format("Service returned status = [%d][url=%s]",
                            response.getStatus(), target.getUri().toString()));
                }
                if (response.hasEntity())
                    return response.readEntity(type);
                throw new ConnectionError(
                        String.format("Service response entity was null. [service=%s]", target.getUri().toString()));
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConnectionError(ex);
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
        try {
            JerseyWebTarget target = connection.connect(path);
            if (params != null && !params.isEmpty()) {
                for (String param : params) {
                    target = target.path(param);
                }
            }
            if (Strings.isNullOrEmpty(mediaType)) {
                mediaType = MediaType.APPLICATION_JSON;
            }
            Retry retry = registry.retry(target.getUri().toString());
            DeleteCallable callable = new DeleteCallable(this, target, mediaType);
            try (Response response = retry.executeCallable(callable)) {
                int rc = response.getStatus();
                if (rc != HttpStatus.SC_OK) {
                    throw new ConnectionError(String.format("Service returned status = [%d][url=%s]",
                            response.getStatus(), target.getUri().toString()));
                }
                if (response.hasEntity())
                    return response.readEntity(type);
                throw new ConnectionError(
                        String.format("Service response entity was null. [service=%s]", target.getUri().toString()));
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConnectionError(ex);
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class PutCallable<R> implements Callable<Response> {
        private final WebServiceClient client;
        private final JerseyWebTarget target;
        private final String mediaType;
        private final R request;

        public PutCallable(WebServiceClient client,
                           JerseyWebTarget target,
                           R request,
                           String mediaType) {
            this.client = client;
            this.target = target;
            this.request = request;
            this.mediaType = mediaType;
        }

        @Override
        public Response call() throws Exception {
            Invocation.Builder builder = client.connection.build(target, mediaType);
            return builder.put(Entity.entity(request, mediaType));
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class PostCallable<R> implements Callable<Response> {
        private final WebServiceClient client;
        private final JerseyWebTarget target;
        private final String mediaType;
        private final R request;

        public PostCallable(WebServiceClient client,
                            JerseyWebTarget target,
                            R request,
                            String mediaType) {
            this.client = client;
            this.target = target;
            this.request = request;
            this.mediaType = mediaType;
        }

        @Override
        public Response call() throws Exception {
            Invocation.Builder builder = client.connection.build(target, mediaType);
            return builder.post(Entity.entity(request, mediaType));
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class DeleteCallable implements Callable<Response> {
        private final WebServiceClient client;
        private final JerseyWebTarget target;
        private final String mediaType;

        public DeleteCallable(WebServiceClient client, JerseyWebTarget target, String mediaType) {
            this.client = client;
            this.target = target;
            this.mediaType = mediaType;
        }

        @Override
        public Response call() throws Exception {
            Invocation.Builder builder = target.request(mediaType);
            return builder.delete();
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class GetCallable implements Callable<Response> {
        private final WebServiceClient client;
        private final JerseyWebTarget target;
        private final String mediaType;

        public GetCallable(WebServiceClient client, JerseyWebTarget target, String mediaType) {
            this.client = client;
            this.target = target;
            this.mediaType = mediaType;
        }

        @Override
        public Response call() throws Exception {
            Invocation.Builder builder = target.request(mediaType);
            return builder.get();
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
            public static final String CONFIG_WAIT_TIME = "waitTime";
        }

        @Config(name = Constants.CONFIG_NAME)
        private String name;
        @Config(name = Constants.CONFIG_CONNECTION)
        private String connection;
        @Config(name = Constants.CONFIG_PATH_MAP, type = Map.class)
        private Map<String, String> pathMap;
        @Config(name = Constants.CONFIG_RETRIES, required = false, type = Integer.class)
        private int retryCount = 0;
        @Config(name = Constants.CONFIG_WAIT_TIME, required = false, parser = TimeValueParser.class)
        private TimeUnitValue waitTime = new TimeUnitValue(3, TimeUnit.SECONDS);
    }

    public static class WebServiceClientConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "client";


        public WebServiceClientConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, String.format("%s.%s", path, __CONFIG_PATH), WebServiceClientSettings.class);
        }
    }
}
