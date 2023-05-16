package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class FileSystemManager {
    public static final String __CONFIG_PATH = "fs";
    public static final String __CONFIG_PATH_DEFS = "fileSystems";

    private BaseEnv<?> env;
    @Getter(AccessLevel.NONE)
    private final Map<String, FileSystem> fileSystems = new HashMap<>();
    private ZookeeperConnection zkConnection;
    private String zkBasePath;
    private FileSystemManagerSettings settings;

    public FileSystemManager init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                  @NonNull BaseEnv<?> env) throws IOException {
        try {
            this.env = env;
            ConfigReader reader = new ConfigReader(config, __CONFIG_PATH, FileSystemManagerSettings.class);
            reader.read();
            settings = (FileSystemManagerSettings) reader.settings();
            zkConnection = env.connectionManager().getConnection(settings.zkConnection, ZookeeperConnection.class);
            if (zkConnection == null) {
                throw new IOException(
                        String.format("ZooKeeper connection not found. [name=%s]", settings.zkConnection));
            }
            if (!zkConnection.isConnected()) {
                zkConnection.connect();
            }
            zkBasePath = new PathUtils.ZkPathBuilder(settings.zkBasePath)
                    .withPath(__CONFIG_PATH)
                    .build();
            DistributedLock lock = getLock();
            lock.lock();
            try {
                readConfig(reader.config());
                readConfig();
                if (settings.autoSave) {
                    save();
                }
            } finally {
                lock.unlock();
            }
            return this;
        } catch (Exception ex) {
            fileSystems.clear();
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            DefaultLogger.stacktrace(ex);
            throw new IOException(ex);
        }
    }

    private DistributedLock getLock() throws Exception {
        return env.createCustomLock(zkBasePath, zkConnection, 10000);
    }

    private void readConfig(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        if (ConfigReader.checkIfNodeExists(config, __CONFIG_PATH_DEFS)) {
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(__CONFIG_PATH_DEFS);
            if (nodes != null && !nodes.isEmpty()) {
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    ConfigReader reader = new ConfigReader(node, FileSystem.FileSystemSettings.class);
                    reader.read();
                    FileSystem.FileSystemSettings settings = (FileSystem.FileSystemSettings) reader.settings();
                    addFs(settings);
                }
            }
        }
    }

    private void readConfig() throws Exception {
        CuratorFramework client = zkConnection.client();
        if (client.checkExists().forPath(zkBasePath) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zkBasePath);
            return;
        }
        List<String> nodes = client.getChildren().forPath(zkBasePath);
        if (nodes != null && !nodes.isEmpty()) {
            for (String node : nodes) {
                String path = new PathUtils.ZkPathBuilder(zkBasePath)
                        .withPath(node)
                        .build();
                FileSystem.FileSystemSettings settings = read(path, client);
                if (settings != null) {
                    addFs(settings);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private FileSystem addFs(FileSystem.FileSystemSettings settings) throws Exception {
        Class<? extends FileSystem> cls = (Class<? extends FileSystem>) Class.forName(settings.type());
        FileSystem fs = cls.getDeclaredConstructor().newInstance();
        fs.init(settings, env);
        fileSystems.put(settings.name(), fs);
        DefaultLogger.LOGGER.info(String.format("Loaded file system. [name=%s][id=%s]", settings.name(), fs.id()));

        return fs;
    }

    @SuppressWarnings("unchecked")
    public <T extends FileSystem> T get(@NonNull String name) {
        return (T) fileSystems.get(name);
    }

    @SuppressWarnings("unchecked")
    public <T extends FileSystem> T add(FileSystem.FileSystemSettings settings) throws IOException {
        Preconditions.checkNotNull(zkConnection);
        try {
            DistributedLock lock = getLock();
            lock.lock();
            try {
                FileSystem fs = addFs(settings);
                CuratorFramework client = zkConnection.client();
                save(settings.name(), settings, client);
                return (T) fs;
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void save() throws IOException {
        Preconditions.checkNotNull(zkConnection);
        try {
            if (!fileSystems.isEmpty()) {
                DistributedLock lock = getLock();
                lock.lock();
                try {
                    CuratorFramework client = zkConnection.client();
                    for (String name : fileSystems.keySet()) {
                        FileSystem fs = fileSystems.get(name);
                        FileSystem.FileSystemSettings settings = fs.settings;
                        save(name, settings, client);
                        DefaultLogger.LOGGER.info(String.format("Saved fileSystem settings. [name=%s]", name));
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private void save(String name,
                      FileSystem.FileSystemSettings settings,
                      CuratorFramework client) throws Exception {
        String path = new PathUtils.ZkPathBuilder(zkBasePath)
                .withPath(name)
                .build();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(path);
        }
        byte[] data = JSONUtils.asBytes(settings, settings.getClass());
        client.setData().forPath(path, data);
    }

    private FileSystem.FileSystemSettings read(String path,
                                               CuratorFramework client) throws Exception {
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                return JSONUtils.read(data, FileSystem.FileSystemSettings.class);
            }
        }
        return null;
    }

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class FileSystemManagerSettings extends Settings {
        @Config(name = "zkPath")
        private String zkBasePath;
        @Config(name = "zkConnection")
        private String zkConnection;
        @Config(name = "autoSave", required = false, type = Boolean.class)
        private boolean autoSave = true;
    }
}
