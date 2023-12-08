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

package io.zyient.core.persistence.impl.solr;

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
import io.zyient.core.persistence.AbstractConnection;
import io.zyient.core.persistence.impl.settings.solr.SolrConnectionSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class SolrConnection extends AbstractConnection<SolrClient> {
    private ISolrAuthHandler authHandler = null;
    private BaseEnv<?> env;
    private final Map<String, SolrClient> clients = new HashMap<>();

    public SolrConnection() {
        super(EConnectionType.solr, SolrConnectionSettings.class);
    }

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
            state().error(ex);
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
            state().error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof SolrConnectionSettings);
        try {
            this.env = env;
            state().setState(EConnectionState.Initialized);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            throw new ConnectionError(ex);
        }
    }

    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkNotNull(settings);
        Preconditions.checkState(!state().hasError());
        synchronized (state()) {
            state().setState(EConnectionState.Connected);
            return this;
        }
    }

    public SolrClient connect(@NonNull String collection) throws ConnectionError {
        Preconditions.checkState(settings instanceof SolrConnectionSettings);
        Preconditions.checkState(state().isConnected());
        synchronized (state()) {
            try {
                if (clients.containsKey(collection)) {
                    return clients.get(collection);
                }
                SolrClient client = null;
                switch (((SolrConnectionSettings) settings).getClientType()) {
                    case Basic -> {
                        client = buildBasicClient(collection);
                    }
                    case Cloud -> {
                        client = buildCloudClient(collection);
                    }
                    case Concurrent -> {
                        client = buildConcurrentClient(collection);
                    }
                    case LoadBalanced -> {
                        client = buildLBClient(collection);
                    }
                }
                if (((SolrConnectionSettings) settings).getAuthHandler() != null) {
                    authHandler = ((SolrConnectionSettings) settings).getAuthHandler()
                            .getDeclaredConstructor()
                            .newInstance();
                    authHandler.init(client, (SolrConnectionSettings) settings);
                }
                clients.put(collection, client);
                return client;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    private SolrClient buildBasicClient(String collection) throws Exception {
        SolrConnectionSettings settings = (SolrConnectionSettings) settings();
        String url = String.format("%s/%s", settings.getUrls().get(0), collection);
        return new Http2SolrClient.Builder(url)
                .withConnectionTimeout(settings.getConnectionTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .withRequestTimeout(settings.getRequestTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build();
    }

    private SolrClient buildLBClient(String collection) throws Exception {
        SolrConnectionSettings settings = (SolrConnectionSettings) settings();
        String url = String.format("%s/%s", settings.getUrls().get(0), collection);
        Http2SolrClient solr = new Http2SolrClient.Builder(url)
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
            arr[ii] = String.format("%s/%s", settings.getUrls().get(ii), collection);
        }
        return new LBHttp2SolrClient.Builder(solr, arr)
                .setAliveCheckInterval(liveCheck, TimeUnit.MILLISECONDS)
                .build();
    }

    private SolrClient buildCloudClient(String collection) throws Exception {
        SolrConnectionSettings settings = (SolrConnectionSettings) settings();
        List<String> arr = new ArrayList<>(settings.getUrls().size());
        for (String url : settings.getUrls()) {
            arr.add(String.format("%s/%s", url, collection));
        }
        return new CloudHttp2SolrClient.Builder(arr)
                .withZkConnectTimeout((int) settings.getConnectionTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .withRetryExpiryTime((int) settings.getRequestTimeout().normalized(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build();
    }

    private SolrClient buildConcurrentClient(String collection) throws Exception {
        SolrConnectionSettings settings = (SolrConnectionSettings) settings();
        String url = String.format("%s/%s", settings.getUrls().get(0), collection);
        Http2SolrClient solr = new Http2SolrClient.Builder(url)
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
        return new ConcurrentUpdateHttp2SolrClient.Builder(settings.getUrls().get(0), solr)
                .withQueueSize(queueSize)
                .withThreadCount(threads)
                .build();
    }

    @Override
    public boolean hasTransactionSupport() {
        return false;
    }

    @Override
    public void close(@NonNull SolrClient connection) throws ConnectionError {
        synchronized (state()) {
            try {
                String key = null;
                for (String name : clients.keySet()) {
                    SolrClient sc = clients.get(name);
                    if (sc.equals(connection)) {
                        key = name;
                        break;
                    }
                }
                if (!Strings.isNullOrEmpty(key)) {
                    clients.remove(key);
                }
                connection.close();
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
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
        synchronized (state()) {
            if (!clients.isEmpty()) {
                for (SolrClient client : clients.values()) {
                    client.close();
                }
                clients.clear();
            }
            if (state().isConnected()) {
                state().setState(EConnectionState.Closed);
            }
        }
    }
}
