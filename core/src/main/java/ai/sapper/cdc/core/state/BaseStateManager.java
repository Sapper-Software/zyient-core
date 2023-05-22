package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.CDCAgentState;
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
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class BaseStateManager implements Closeable {
    public static class Constants {
        public static final int LOCK_RETRY_COUNT = 8;
        public static final String ZK_PATH_HEARTBEAT = "/heartbeat";
        public static final String ZK_PATH_PROCESS_STATE = "state";

        public static final String LOCK_STATE = "__state";
    }

    private ZookeeperConnection connection;
    private BaseStateManagerSettings settings;
    private String zkPath;
    private String zkAgentStatePath;
    private String zkModulePath;
    private String zkAgentPath;

    private ModuleInstance moduleInstance;
    private String name;
    private String environment;
    @Getter(AccessLevel.NONE)
    private DistributedLock stateLock;
    private final Map<String, OffsetStateManager<?>> offsetManagers = new HashMap<>();

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
        init(env, (BaseStateManagerSettings) reader.settings());
    }

    protected void init(@NonNull BaseEnv<?> env,
                        @NonNull BaseStateManagerSettings settings) throws Exception {
        Preconditions.checkNotNull(moduleInstance);
        Preconditions.checkState(!Strings.isNullOrEmpty(environment));

        this.settings = settings;

        connection = env.connectionManager()
                .getConnection(settings.getZkConnection(),
                        ZookeeperConnection.class);
        Preconditions.checkNotNull(connection);
        if (!connection.isConnected()) connection.connect();
        CuratorFramework client = connection().client();
        zkPath = new PathUtils.ZkPathBuilder(basePath())
                .withPath(environment)
                .build();
        zkModulePath = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(moduleInstance.getModule())
                .build();
        zkAgentPath = new PathUtils.ZkPathBuilder(zkModulePath)
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

        initOffsetManagers();
    }

    private void initOffsetManagers() {

    }

    public abstract BaseStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                          @NonNull BaseEnv<?> env) throws StateManagerError;

    public synchronized void checkState() {
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
                               @NonNull CDCAgentState.AgentState state) throws StateManagerError {
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
                heartbeat.setType(state.getClass().getCanonicalName());
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
