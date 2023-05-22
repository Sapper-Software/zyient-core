package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.processing.ProcessorState;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public abstract class OffsetStateManager<T extends Offset> {
    private final ProcessorState state = new ProcessorState();
    private ZookeeperConnection connection;
    private OffsetStateManagerSettings settings;
    private String zkPath;
    private BaseEnv<?> env;
    @Getter(AccessLevel.NONE)
    protected DistributedLock stateLock;

    public abstract OffsetStateManager<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                               @NonNull BaseEnv<?> env) throws StateManagerError;

    public abstract OffsetStateManager<T> init(@NonNull OffsetStateManagerSettings settings,
                                               @NonNull BaseEnv<?> env) throws StateManagerError;

    protected void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                        @NonNull BaseEnv<?> env,
                        @NonNull Class<? extends OffsetStateManagerSettings> type) throws Exception {
        ConfigReader reader = new ConfigReader(xmlConfig, type);
        init(env, reader);
    }

    protected void init(@NonNull BaseEnv<?> env,
                        @NonNull ConfigReader reader) throws Exception {
        reader.read();
        setup((OffsetStateManagerSettings) reader.settings(), env);
    }

    protected void setup(@NonNull OffsetStateManagerSettings settings,
                         @NonNull BaseEnv<?> env) throws Exception {
        this.settings = settings;

        connection = env.connectionManager().getConnection(settings.getZkConnection(), ZookeeperConnection.class);
        if (connection == null) {
            throw new Exception(String.format("ZooKeeper connection not found. [name=%s]", settings.getZkConnection()));
        }
        if (!connection.isConnected()) {
            connection.connect();
        }
        zkPath = new PathUtils.ZkPathBuilder(settings.getBasePath())
                .withPath(settings.getName())
                .build();
        CuratorFramework client = connection.client();
        if (client.checkExists().forPath(zkPath) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zkPath);
        }

        stateLock = getStateLock(client);
        this.env = env;
    }

    private DistributedLock getStateLock(@NonNull CuratorFramework client) throws Exception {
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(settings.getName(), zp, connection, settings.getLockTimeout());
    }

    protected DistributedLock getLock(@NonNull String type,
                                      @NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isRunning());
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(type)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(type, zp, connection, settings.getLockTimeout());
    }

    protected DistributedLock getLock(@NonNull String type,
                                      @NonNull String name,
                                      @NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isRunning());
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(type)
                .withPath(name)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(type, zp, connection, settings.getLockTimeout());
    }

    protected String registerType(@NonNull String type) throws StateManagerError {
        Preconditions.checkState(state.isRunning());
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(type)
                .build();
        stateLock.lock();
        try {
            CuratorFramework client = connection.client();
            if (client.checkExists().forPath(zp) == null) {
                client.create().creatingParentContainersIfNeeded().forPath(zp);
            }
            return zp;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        } finally {
            stateLock.unlock();
        }
    }

    protected <R extends OffsetState<?, T>> R get(@NonNull String type,
                                                  @NonNull String name,
                                                  @NonNull Class<? extends R> offsetType) throws StateManagerError {
        Preconditions.checkState(state.isRunning());
        CuratorFramework client = connection.client();
        return get(type, name, offsetType, client);
    }

    protected <R extends OffsetState<?, T>> R get(String type,
                                                  String name,
                                                  @NonNull Class<? extends R> offsetType,
                                                  CuratorFramework client) throws StateManagerError {
        try {
            String path = new PathUtils.ZkPathBuilder(zkPath)
                    .withPath(type)
                    .withPath(name)
                    .build();
            return JSONUtils.read(client, path, offsetType);
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    protected <R extends OffsetState<?, T>> R create(@NonNull String type,
                                                     @NonNull String name,
                                                     @NonNull Class<? extends R> offsetType) throws StateManagerError {
        Preconditions.checkState(state.isRunning());
        try {
            String zp = registerType(type);
            CuratorFramework client = connection.client();
            try (DistributedLock lock = getLock(type, client)) {
                lock.lock();
                try {
                    R current = get(type,
                            name,
                            offsetType,
                            client);
                    if (current != null) {
                        return current;
                    }
                    current = offsetType.getDeclaredConstructor().newInstance();
                    current.setType(type);
                    current.setName(name);
                    current.setTimeCreated(System.currentTimeMillis());
                    current.setTimeUpdated(System.currentTimeMillis());
                    current.setLastUpdatedBy(env.moduleInstance());
                    zp = new PathUtils.ZkPathBuilder(zp)
                            .withPath(name)
                            .build();
                    if (client.checkExists().forPath(zp) == null) {
                        client.create().creatingParentContainersIfNeeded().forPath(zp);
                    }
                    client.setData().forPath(zp, JSONUtils.asBytes(current, offsetType));

                    return current;
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    @SuppressWarnings("unchecked")
    protected <R extends OffsetState<?, T>> R update(@NonNull R offset) throws StateManagerError {
        Preconditions.checkState(state.isRunning());
        try {
            CuratorFramework client = connection.client();
            try (DistributedLock lock = getLock(offset.getType(), offset.getName(), client)) {
                lock.lock();
                try {
                    R current = (R) get(offset.getType(),
                            offset.getName(),
                            offset.getClass(),
                            client);
                    if (current == null) {
                        throw new StateManagerError(
                                String.format("Offset not found. [type=%s][name=%s]",
                                        offset.getType(), offset.getName()));
                    }
                    if (current.getTimeUpdated() > offset.getTimeUpdated()) {
                        throw new StateManagerError(
                                String.format("Offset is stale. [type=%s][name=%s]",
                                        offset.getType(), offset.getName()));
                    }
                    String zp = new PathUtils.ZkPathBuilder(zkPath)
                            .withPath(offset.getType())
                            .withPath(offset.getName())
                            .build();
                    offset.setTimeUpdated(System.currentTimeMillis());
                    offset.setLastUpdatedBy(env.moduleInstance());
                    client.setData().forPath(zp, JSONUtils.asBytes(offset, offset.getClass()));

                    return offset;
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public boolean delete(@NonNull String type,
                          @NonNull String name,
                          @NonNull Class<? extends OffsetState<?, T>> offsetType) throws StateManagerError {
        Preconditions.checkState(state.isRunning());
        try {
            CuratorFramework client = connection.client();
            try (DistributedLock lock = getLock(type, client)) {
                lock.lock();
                try {
                    OffsetState<?, T> current = get(type,
                            name,
                            offsetType,
                            client);
                    if (current == null) {
                        return false;
                    }
                    String zp = new PathUtils.ZkPathBuilder(zkPath)
                            .withPath(type)
                            .withPath(name)
                            .build();
                    client.delete().deletingChildrenIfNeeded().forPath(zp);
                    return true;
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public String name() {
        return settings.getName();
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends OffsetStateManager<?>> parseManagerType(
            @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        String type = xmlConfig.getString(OffsetStateManagerSettings.Constants.CONFIG_TYPE);
        ConfigReader.checkStringValue(type, OffsetStateManager.class, OffsetStateManagerSettings.Constants.CONFIG_TYPE);
        return (Class<? extends OffsetStateManager<?>>) Class.forName(type);
    }
}
