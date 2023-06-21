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

package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.ESettingsSource;
import ai.sapper.cdc.core.model.Heartbeat;
import ai.sapper.cdc.core.model.ModuleInstance;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class BaseStateManager implements Closeable {
    public static class Constants {
        public static final String ZK_PROCESSORS_NODE = "processors";
        public static final int LOCK_RETRY_COUNT = 8;
        public static final String ZK_PATH_HEARTBEAT = "/heartbeat";
        public static final String ZK_PATH_PROCESS_STATE = "state";

        public static final String LOCK_STATE = "__state";
        public static final String ZK_PATH_SHARED_OFFSETS = "offsets";
    }

    private ZookeeperConnection connection;
    private BaseStateManagerSettings settings;
    private String zkPath;
    private String zkAgentStatePath;
    private String zkModulePath;
    private String zkAgentPath;
    private HierarchicalConfiguration<ImmutableNode> config;

    private ModuleInstance moduleInstance;
    private String name;
    private String environment;
    @Getter(AccessLevel.NONE)
    private DistributedLock stateLock;
    private final Map<String, OffsetStateManager<?>> offsetManagers = new HashMap<>();
    private BaseEnv<?> env;

    protected void stateLock() throws Exception {
        int retryCount = 0;
        while (true) {
            try {
                stateLock.lock();
                break;
            } catch (DistributedLock.LockError le) {
                if (retryCount > settings.getLockRetryCount()) {
                    throw new Exception(
                            String.format("Error acquiring lock. [error=%s][retries=%d]",
                                    le.getLocalizedMessage(), retryCount));
                }
                DefaultLogger.warn(String.format("Failed to acquire lock, will retry... [error=%s][retries=%d]",
                        le.getLocalizedMessage(), retryCount));
                Thread.sleep(500);
                retryCount++;
            }
        }
    }

    protected void stateUnlock() {
        stateLock.unlock();
    }

    public BaseStateManager withEnvironment(@NonNull String environment,
                                            @NonNull String name) {
        this.environment = environment;
        this.name = name;

        return this;
    }


    public BaseStateManager withModuleInstance(@NonNull ModuleInstance moduleInstance) {
        this.moduleInstance = moduleInstance;
        return this;
    }

    public String basePath() {
        return settings().getBasePath();
    }

    protected void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                        @NonNull String path,
                        @NonNull BaseEnv<?> env,
                        @NonNull Class<? extends BaseStateManagerSettings> settingsType) throws Exception {
        ConfigReader reader = new ConfigReader(xmlConfig, path, settingsType);
        init(env, reader);
    }

    protected void init(@NonNull BaseEnv<?> env,
                        @NonNull ConfigReader reader) throws Exception {
        reader.read();
        config = reader.config();
        init(env, (BaseStateManagerSettings) reader.settings());
    }

    protected void init(@NonNull BaseEnv<?> env,
                        @NonNull BaseStateManagerSettings settings) throws Exception {
        Preconditions.checkNotNull(moduleInstance);
        Preconditions.checkState(!Strings.isNullOrEmpty(environment));

        this.settings = settings;
        this.env = env;

        connection = env.connectionManager()
                .getConnection(settings.getZkConnection(),
                        ZookeeperConnection.class);
        Preconditions.checkNotNull(connection);
        if (!connection.isConnected()) connection.connect();
        CuratorFramework client = connection().client();
        String zp = new PathUtils.ZkPathBuilder(env.zkBasePath())
                .withPath(settings.getBasePath())
                .build();
        settings.setBasePath(zp);

        zkPath = new PathUtils.ZkPathBuilder(basePath())
                .withPath(environment)
                .build();
        zkModulePath = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(moduleInstance.getModule())
                .build();
        zkAgentPath = new PathUtils.ZkPathBuilder(zkModulePath)
                .withPath(Constants.ZK_PROCESSORS_NODE)
                .withPath(moduleInstance.getName())
                .build();

        stateLock = env.createLock(zkPath,
                moduleInstance.getModule(),
                Constants.LOCK_STATE);
        if (stateLock == null) {
            throw new ConfigurationException(
                    String.format("Replication Lock not defined. [name=%s]",
                            Constants.LOCK_STATE));
        }

        if (client.checkExists().forPath(zkAgentPath) == null) {
            String path = client.create().creatingParentContainersIfNeeded().forPath(zkAgentPath);
            if (Strings.isNullOrEmpty(path)) {
                throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
            }
        }
        zkAgentStatePath = new PathUtils.ZkPathBuilder(zkAgentPath)
                .withPath(Constants.ZK_PATH_PROCESS_STATE)
                .build();

        initOffsetManagers(client);
    }

    private void initOffsetManagers(CuratorFramework client) throws Exception {
        if (config != null) {
            readOffsetManagers(config);
        }
        readOffsetManagers(client);
        if (settings.isSaveOffsetManager() && !offsetManagers.isEmpty()) {
            stateLock.lock();
            try {
                for (String name : offsetManagers.keySet()) {
                    OffsetStateManager<?> manager = offsetManagers.get(name);
                    OffsetStateManagerSettings os = manager.settings();
                    if (os.getSource() != ESettingsSource.File) continue;
                    saveOffsetManager(os, client);
                }
            } finally {
                stateLock.unlock();
            }
        }
    }

    private void readOffsetManagers(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        if (ConfigReader.checkIfNodeExists(config, BaseStateManagerSettings.__CONFIG_PATH_OFFSET_MANAGERS)) {
            String s = config.getString(BaseStateManagerSettings.Constants.CONFIG_SAVE_OFFSETS);
            if (!Strings.isNullOrEmpty(s)) {
                settings.setSaveOffsetManager(Boolean.parseBoolean(s));
            }
            HierarchicalConfiguration<ImmutableNode> root
                    = config.configurationAt(BaseStateManagerSettings.__CONFIG_PATH_OFFSET_MANAGERS);
            List<HierarchicalConfiguration<ImmutableNode>> nodes
                    = root.configurationsAt(OffsetStateManagerSettings.__CONFIG_PATH);
            if (nodes != null && !nodes.isEmpty()) {
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    Class<? extends OffsetStateManager<?>> cls = OffsetStateManager.parseManagerType(node);
                    OffsetStateManager<?> manager = cls
                            .getDeclaredConstructor()
                            .newInstance()
                            .withStateManager(this)
                            .init(node, env);
                    offsetManagers.put(manager.name(), manager);
                    DefaultLogger.info(String.format("[Offset Manager] Read from file: [type=%s][name=%s]",
                            manager.settings().getType(), manager.settings().getName()));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readOffsetManagers(CuratorFramework client) throws Exception {
        String zkPath = new PathUtils.ZkPathBuilder(basePath())
                .withPath(BaseStateManagerSettings.__CONFIG_PATH_OFFSET_MANAGERS)
                .withPath(OffsetStateManagerSettings.__CONFIG_PATH)
                .build();
        if (client.checkExists().forPath(zkPath) == null) return;
        List<String> nodes = client.getChildren().forPath(zkPath);
        if (nodes != null && !nodes.isEmpty()) {
            for (String node : nodes) {
                String cp = new PathUtils.ZkPathBuilder(zkPath)
                        .withPath(node)
                        .build();
                OffsetStateManagerSettings os = JSONUtils.read(client, cp, OffsetStateManagerSettings.class);
                if (os != null) {
                    os.setSource(ESettingsSource.ZooKeeper);
                    OffsetStateManager<?> manager = os.getType()
                            .getDeclaredConstructor()
                            .newInstance()
                            .withStateManager(this)
                            .init(os, env);
                    offsetManagers.put(manager.name(), manager);
                    DefaultLogger.info(String.format("[Offset Manager] Read from ZooKeeper: [type=%s][name=%s]",
                            manager.settings().getType(), manager.settings().getName()));
                }
            }
        }
    }


    private void saveOffsetManager(OffsetStateManagerSettings settings,
                                   CuratorFramework client) throws Exception {
        String zkPath = getOffsetSettingsPath(settings.getName());
        if (client.checkExists().forPath(zkPath) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zkPath);
        }
        client.setData().forPath(zkPath, JSONUtils.asBytes(settings, settings.getClass()));
    }

    @SuppressWarnings("unchecked")
    public <T extends OffsetStateManager<?>> T getOffsetManager(@NonNull String name,
                                                                @NonNull Class<? extends T> managerType) {
        OffsetStateManager<?> manager = offsetManagers.get(name);
        if (manager != null) {
            if (manager.getClass().equals(managerType)
                    || ReflectionUtils.isSuperType(managerType, manager.getClass())) {
                return (T) manager;
            }
        }
        return null;
    }

    public void updateOffsetManager(@NonNull OffsetStateManagerSettings settings) throws StateManagerError {
        CuratorFramework client = connection.client();
        stateLock.lock();
        try {
            saveOffsetManager(settings, client);
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        } finally {
            stateLock.unlock();
        }
    }

    public boolean deleteOffsetManager(@NonNull String name) throws StateManagerError {
        CuratorFramework client = connection.client();
        stateLock.lock();
        try {
            if (offsetManagers.containsKey(name)) {
                offsetManagers.remove(name);
            }
            String zkPath = getOffsetSettingsPath(name);
            if (client.checkExists().forPath(zkPath) == null) {
                return false;
            }
            client.delete().deletingChildrenIfNeeded().forPath(zkPath);
            return true;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        } finally {
            stateLock.unlock();
        }
    }

    private String getOffsetSettingsPath(@NonNull String name) {
        return new PathUtils.ZkPathBuilder(basePath())
                .withPath(BaseStateManagerSettings.__CONFIG_PATH_OFFSET_MANAGERS)
                .withPath(OffsetStateManagerSettings.__CONFIG_PATH)
                .withPath(name)
                .build();
    }

    public String getOffsetPath(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        return new PathUtils.ZkPathBuilder(zkModulePath)
                .withPath(Constants.ZK_PATH_SHARED_OFFSETS)
                .withPath(name)
                .build();
    }

    public <T extends Offset> T readOffset(@NonNull String name,
                                           @NonNull Class<? extends T> type) throws StateManagerError {
        checkState();
        String path = getOffsetPath(name);
        try {
            CuratorFramework client = connection.client();
            return JSONUtils.read(client, path, type);
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public <T extends Offset> T checkAndCreateOffset(@NonNull String name,
                                                     @NonNull Class<? extends T> type) throws StateManagerError {
        checkState();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        try {
            stateLock.lock();
            try {
                T offset = readOffset(name, type);
                if (offset != null) {
                    return offset;
                }
                offset = type.getDeclaredConstructor().newInstance();
                return saveOffset(name, offset);
            } finally {
                stateLock.unlock();
            }
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Offset> T updateOffset(@NonNull String name,
                                             @NonNull T offset) throws StateManagerError {
        checkState();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        try {
            stateLock.lock();
            try {
                T current = (T) readOffset(name, offset.getClass());
                if (current == null) {
                    throw new StateManagerError(String.format("Offset not found. [name=%s]", name));
                }
                if (current.getTimeUpdated() > offset.getTimeUpdated()) {
                    throw new StateManagerError(String.format("[%s] Offset instance is stale...", name));
                }
                return saveOffset(name, offset);
            } finally {
                stateLock.unlock();
            }
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    private <T extends Offset> T saveOffset(@NonNull String name,
                                            @NonNull T offset) throws Exception {
        String path = getOffsetPath(name);
        CuratorFramework client = connection.client();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(path);
        }
        offset.setTimeUpdated(System.currentTimeMillis());
        client.setData().forPath(path, JSONUtils.asBytes(offset, offset.getClass()));
        return offset;
    }

    public abstract BaseStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                          @NonNull BaseEnv<?> env) throws StateManagerError;

    public void checkState() {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
    }

    @Override
    public void close() throws IOException {
        if (stateLock != null) {
            stateLock.close();
        }
    }

    public abstract Heartbeat heartbeat(@NonNull String instance) throws StateManagerError;

    public Heartbeat heartbeat(@NonNull String name,
                               @NonNull Class<?> caller,
                               @NonNull AbstractState<?> state) throws StateManagerError {
        checkState();
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                String path = getHeartbeatPath(name);
                if (client.checkExists().forPath(path) == null) {
                    path = client.create().creatingParentContainersIfNeeded().forPath(path);
                    if (Strings.isNullOrEmpty(path)) {
                        throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                    }
                }
                Heartbeat heartbeat = new Heartbeat();
                heartbeat.setName(name);
                heartbeat.setType(caller.getCanonicalName());
                heartbeat.setState(state.getState().name());
                if (state.hasError()) {
                    heartbeat.setError(state.getError());
                }
                heartbeat.setTimestamp(System.currentTimeMillis());
                heartbeat.setModule(moduleInstance);
                String json = JSONUtils.asString(heartbeat, Heartbeat.class);
                client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

                return heartbeat;
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    public Heartbeat heartbeat(@NonNull String name,
                               @NonNull String state) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        try {
            CuratorFramework client = connection().client();
            String path = getHeartbeatPath(name);
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    String json = new String(data, StandardCharsets.UTF_8);
                    return JSONUtils.read(json, Heartbeat.class);
                } else {
                    Heartbeat hb = new Heartbeat();
                    hb.setName(name);
                    hb.setModule(moduleInstance);
                    hb.setState(state);

                    String json = JSONUtils.asString(hb, Heartbeat.class);
                    client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

                    return hb;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }


    private String getHeartbeatPath(String name) {
        return new PathUtils.ZkPathBuilder(zkModulePath)
                .withPath(Constants.ZK_PATH_HEARTBEAT)
                .withPath(name)
                .build();
    }
}
