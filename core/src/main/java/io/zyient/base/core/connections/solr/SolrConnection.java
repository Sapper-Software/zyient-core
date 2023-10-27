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

package io.zyient.base.core.connections.solr;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.ZkConfigReader;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ConnectionError;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.base.core.connections.settings.solr.SolrConnectionSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class SolrConnection implements Connection {
    private SolrConnectionSettings settings;
    @Getter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();
    private SolrClient client;
    private ISolrAuthHandler authHandler = null;
    private BaseEnv<?> env;

    @Override
    public String name() {
        Preconditions.checkNotNull(settings);
        return settings.getName();
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            ConfigReader reader = new ConfigReader(config,
                    SolrConnectionSettings.__CONFIG_PATH,
                    SolrConnectionSettings.class);
            reader.read();
            settings = (SolrConnectionSettings) reader.settings();
            settings.validate();

            return setup(settings, env);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        try {
            CuratorFramework client = connection.client();
            String zkPath = new PathUtils.ZkPathBuilder(path)
                    .withPath(SolrConnectionSettings.__CONFIG_PATH)
                    .build();
            ZkConfigReader reader = new ZkConfigReader(client, SolrConnectionSettings.class);
            if (!reader.read(zkPath)) {
                throw new ConnectionError(
                        String.format("WebService Connection settings not found. [path=%s]", zkPath));
            }
            settings = (SolrConnectionSettings) reader.settings();
            settings.validate();
            return setup(settings, env);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof SolrConnectionSettings);
        try {
            this.env = env;
            state.setState(EConnectionState.Initialized);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkNotNull(settings);
        synchronized (state) {
            if (state.isConnected()) return this;
            try {
                switch (settings.getClientType()) {
                    case Basic -> {
                        buildBasicClient();
                    }
                    case Cloud -> {
                        buildCloudClient();
                    }
                    case Concurrent -> {
                        buildConcurrentClient();
                    }
                    case LoadBalanced -> {
                        buildLBClient();
                    }
                }
                if (settings.getAuthHandler() != null) {
                    authHandler = settings.getAuthHandler()
                            .getDeclaredConstructor()
                            .newInstance();
                    authHandler.init(client, settings);
                }
                state.setState(EConnectionState.Connected);
                return this;
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError(ex);
            }
        }
    }

    private void buildBasicClient() throws Exception {
        client = new Http2SolrClient.Builder(settings.getUrls().get(0))
                .withResponseParser(new JsonMapResponseParser())
                .withConnectionTimeout(settings.getConnectionTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .withRequestTimeout(settings.getRequestTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build();
    }

    private void buildLBClient() throws Exception {
        Http2SolrClient solr = new Http2SolrClient.Builder(settings.getUrls().get(0))
                .withResponseParser(new JsonMapResponseParser())
                .withConnectionTimeout(settings.getConnectionTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .withRequestTimeout(settings.getRequestTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build();
        int liveCheck = SolrConnectionSettings.ConstantKeys.DEFAULT_LIVE_CHECK_INTERVAL;
        if (settings.getParameters() != null) {
            if (settings.getParameters().containsKey(SolrConnectionSettings.ConstantKeys.KEY_LIVE_CHECK_INTERVAL)) {
                String ss = settings.getParameters().get(SolrConnectionSettings.ConstantKeys.KEY_LIVE_CHECK_INTERVAL);
                if (!Strings.isNullOrEmpty(ss)) {
                    liveCheck = Integer.parseInt(ss);
                }
            }
        }
        String[] arr = new String[settings.getUrls().size()];
        for (int ii = 0; ii < settings.getUrls().size(); ii++) {
            arr[ii] = settings.getUrls().get(ii);
        }
        client = new LBHttp2SolrClient.Builder(solr, arr)
                .setAliveCheckInterval(liveCheck, TimeUnit.MILLISECONDS)
                .build();
    }

    private void buildCloudClient() throws Exception {
        client = new CloudHttp2SolrClient.Builder(settings.getUrls())
                .withResponseParser(new JsonMapResponseParser())
                .withZkConnectTimeout((int) settings.getConnectionTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .withRetryExpiryTime((int) settings.getRequestTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build();
    }

    private void buildConcurrentClient() throws Exception {
        Http2SolrClient solr = new Http2SolrClient.Builder(settings.getUrls().get(0))
                .withResponseParser(new JsonMapResponseParser())
                .withConnectionTimeout(settings.getConnectionTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .withRequestTimeout(settings.getRequestTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build();
        int queueSize = SolrConnectionSettings.ConstantKeys.DEFAULT_QUEUE_SIZE;
        int threads = SolrConnectionSettings.ConstantKeys.DEFAULT_THREADS;
        if (settings.getParameters() != null) {
            if (settings.getParameters().containsKey(SolrConnectionSettings.ConstantKeys.KEY_CONCURRENT_QUEUE_SIZE)) {
                String ss = settings.getParameters().get(SolrConnectionSettings.ConstantKeys.KEY_CONCURRENT_QUEUE_SIZE);
                queueSize = Integer.parseInt(ss);
            }
            if (settings.getParameters().containsKey(SolrConnectionSettings.ConstantKeys.KEY_CONCURRENT_THREADS)) {
                String ss = settings.getParameters().get(SolrConnectionSettings.ConstantKeys.KEY_CONCURRENT_THREADS);
                threads = Integer.parseInt(ss);
            }
        }
        client = new ConcurrentUpdateHttp2SolrClient.Builder(settings.getUrls().get(0), solr)
                .withQueueSize(queueSize)
                .withThreadCount(threads)
                .build();
    }

    @Override
    public Throwable error() {
        return state.getError();
    }

    @Override
    public EConnectionState connectionState() {
        return state.getState();
    }

    @Override
    public boolean isConnected() {
        return state.isConnected();
    }

    @Override
    public String path() {
        return SolrConnectionSettings.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return EConnectionType.solr;
    }

    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (client != null) {
                client.close();
                client = null;
            }
            if (state.isConnected()) {
                state.setState(EConnectionState.Closed);
            }
        }
    }
}
