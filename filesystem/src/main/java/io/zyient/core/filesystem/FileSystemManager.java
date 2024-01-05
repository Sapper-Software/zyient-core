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

package io.zyient.core.filesystem;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.model.ESettingsSource;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.filesystem.model.FileSystemManagerSettings;
import io.zyient.core.filesystem.model.FileSystemSettings;
import io.zyient.core.filesystem.sync.FileSystemSync;
import io.zyient.core.filesystem.sync.FileSystemSyncSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class FileSystemManager implements Closeable {
    public static final String __CONFIG_PATH = "fs";
    public static final String __CONFIG_PATH_DEFS = "fileSystems";
    public static final String __CONFIG_PATH_DEF = "fileSystem";

    private final ProcessorState state = new ProcessorState();
    private BaseEnv<?> env;
    @Getter(AccessLevel.NONE)
    private final Map<String, FileSystem> fileSystems = new HashMap<>();
    private ZookeeperConnection zkConnection;
    private String zkBasePath;
    private FileSystemManagerSettings settings;
    private final Map<String, FileSystemSync> syncers = new HashMap<>();
    private final Map<String, Thread> syncerThreads = new HashMap<>();

    public FileSystemManager init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                  @NonNull BaseEnv<?> env) throws IOException {
        try {
            this.env = env;
            ConfigReader reader = new ConfigReader(config, __CONFIG_PATH, FileSystemManagerSettings.class);
            reader.read();
            settings = (FileSystemManagerSettings) reader.settings();
            zkConnection = env.connectionManager().getConnection(settings.getZkConnection(), ZookeeperConnection.class);
            if (zkConnection == null) {
                throw new IOException(
                        String.format("ZooKeeper connection not found. [name=%s]", settings.getZkConnection()));
            }
            if (!zkConnection.isConnected()) {
                zkConnection.connect();
            }
            zkBasePath = new PathUtils.ZkPathBuilder(settings.getZkBasePath())
                    .withPath(__CONFIG_PATH)
                    .build();
            try (DistributedLock lock = getLock()) {
                lock.lock();
                try {
                    readConfig(reader.config());
                    readConfig();
                    if (settings.isAutoSave()) {
                        save();
                    }
                    readSyncSettings(reader.config());
                } finally {
                    lock.unlock();
                }
            }
            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            fileSystems.clear();
            DefaultLogger.error(ex.getLocalizedMessage());
            DefaultLogger.stacktrace(ex);
            throw new IOException(ex);
        }
    }

    private void readSyncSettings(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        if (ConfigReader.checkIfNodeExists(config, FileSystemSyncSettings.__CONFIG_PATH)) {
            List<HierarchicalConfiguration<ImmutableNode>> nodes
                    = config.configurationsAt(FileSystemSyncSettings.__CONFIG_PATH);
            if (nodes != null && !nodes.isEmpty()) {
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    readSyncConfig(node);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readSyncConfig(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        Class<?> type = ConfigReader.readAsClass(config);
        if (!ReflectionHelper.isSuperType(FileSystemSync.class, type)) {
            throw new Exception(String.format("Invalid File Sync implementation. [type=%s]", type.getCanonicalName()));
        }
        Class<? extends FileSystemSync> syncClass = (Class<? extends FileSystemSync>) type;
        FileSystemSync syncer = syncClass.getDeclaredConstructor().newInstance();
        ConfigReader reader = new ConfigReader(config, syncer.getSettingsType());
        reader.read();
        FileSystemSyncSettings settings = (FileSystemSyncSettings) reader.settings();
        FileSystem fs = get(settings.getFs());
        if (fs == null) {
            throw new Exception(String.format("File System not found. [name=%s]", settings.getFs()));
        }
        syncer.init(settings, fs, env);
        Thread thread = new Thread(syncer, String.format("SYNCER-[%s]", fs.settings.getName()));
        thread.start();
        syncerThreads.put(fs.settings.getName(), thread);
        syncers.put(fs.settings.getName(), syncer);
    }

    private DistributedLock getLock() throws Exception {
        return env.createCustomLock("filesystem-manager", zkBasePath, zkConnection, 10000);
    }


    @SuppressWarnings("unchecked")
    private void readConfig(HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        if (ConfigReader.checkIfNodeExists(config, __CONFIG_PATH_DEFS)) {
            config = config.configurationAt(__CONFIG_PATH_DEFS);
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(__CONFIG_PATH_DEF);
            if (nodes != null && !nodes.isEmpty()) {
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    String type = node.getString(FileSystemSettings.Constants.CONFIG_FS_CLASS);
                    Class<? extends FileSystem> cls = (Class<? extends FileSystem>) Class.forName(type);
                    FileSystem fs = cls.getDeclaredConstructor().newInstance();
                    fs.withZkConnection(zkConnection)
                            .withZkPath(zkBasePath)
                            .init(node, env);
                    fileSystems.put(fs.settings.getName(), fs);
                    DefaultLogger.info(String.format("Loaded file system. [name=%s][id=%s]", fs.settings.getName(), fs.id()));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
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
                FileSystemSettings settings = read(path, client);
                if (settings != null) {
                    if (this.settings.isOverwriteSettings()) {
                        if (fileSystems.containsKey(settings.getName())) {
                            DefaultLogger.info(
                                    String.format("Overwriting with Local settings. [name=%s]", settings.getName()));
                            continue;
                        }
                    }
                    Class<? extends FileSystem> cls = (Class<? extends FileSystem>) Class.forName(settings.getType());
                    FileSystem fs = cls.getDeclaredConstructor().newInstance();
                    addFs(settings, fs);
                }
            }
        }
    }

    private FileSystem addFs(FileSystemSettings settings, FileSystem fs) throws Exception {
        fs.init(settings, env);
        fileSystems.put(settings.getName(), fs);
        DefaultLogger.info(String.format("Loaded file system. [name=%s][id=%s]", settings.getName(), fs.id()));
        return fs;
    }

    @SuppressWarnings("unchecked")
    public FileSystem read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        HierarchicalConfiguration<ImmutableNode> node = xmlConfig.configurationAt(__CONFIG_PATH_DEF);
        if (node == null) {
            throw new Exception(String.format("File System configuration not found. [name=%s]", __CONFIG_PATH_DEF));
        }
        String name = node.getString(FileSystemSettings.Constants.CONFIG_NAME);
        ConfigReader.checkStringValue(name, getClass(), FileSystemSettings.Constants.CONFIG_NAME);
        if (!fileSystems.containsKey(name)) {
            String type = node.getString(FileSystemSettings.Constants.CONFIG_FS_CLASS);
            ConfigReader.checkStringValue(type, getClass(), FileSystemSettings.Constants.CONFIG_FS_CLASS);
            Class<? extends FileSystem> cls = (Class<? extends FileSystem>) Class.forName(type);
            FileSystem fs = cls.getDeclaredConstructor().newInstance();
            fs.init(node, env);
            fileSystems.put(fs.settings.getName(), fs);
            DefaultLogger.info(String.format("Loaded file system. [name=%s][id=%s]", fs.settings.getName(), fs.id()));
        }
        return fileSystems.get(name);
    }

    @SuppressWarnings("unchecked")
    public <T extends FileSystem> T get(@NonNull String name) {
        return (T) fileSystems.get(name);
    }

    public <T extends FileSystem> T add(@NonNull FileSystemSettings settings) throws IOException {
        Preconditions.checkNotNull(zkConnection);
        try (DistributedLock lock = getLock()) {
            lock.lock();
            try {
                return addOrUpdate(settings);
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends FileSystem> T addOrUpdate(FileSystemSettings settings) throws Exception {
        Class<? extends FileSystem> cls = (Class<? extends FileSystem>) Class.forName(settings.getType());
        FileSystem fs = cls.getDeclaredConstructor().newInstance();
        fs = addFs(settings, fs);
        CuratorFramework client = zkConnection.client();
        save(settings.getName(), settings, client);
        return (T) fs;
    }

    public <T extends FileSystem> T update(@NonNull FileSystemSettings settings) throws IOException {
        Preconditions.checkNotNull(zkConnection);
        try (DistributedLock lock = getLock()) {
            lock.lock();
            try {
                FileSystem fs = get(settings.getName());
                if (fs == null) {
                    throw new IOException(String.format("File System not found. [name=%s]", settings.getName()));
                }
                fileSystems.remove(settings.getName());
                return addOrUpdate(settings);
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public boolean remove(@NonNull String name) throws IOException {
        Preconditions.checkNotNull(zkConnection);
        try (DistributedLock lock = getLock()) {
            lock.lock();
            try {
                FileSystem fs = get(name);
                if (fs == null) {
                    return false;
                }
                fileSystems.remove(name);
                String path = fs.zkPath();
                CuratorFramework client = zkConnection.client();
                if (client.checkExists().forPath(path) != null) {
                    client.delete().deletingChildrenIfNeeded().forPath(path);
                    return true;
                }
                return false;
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void saveWithLock() throws IOException {
        try {
            try (DistributedLock lock = getLock()) {
                lock.lock();
                try {
                    save();
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void save() throws IOException {
        Preconditions.checkNotNull(zkConnection);
        if (!fileSystems.isEmpty()) {
            try {
                CuratorFramework client = zkConnection.client();
                for (String name : fileSystems.keySet()) {
                    FileSystem fs = fileSystems.get(name);
                    FileSystemSettings settings = fs.settings;
                    if (settings.getSource() == ESettingsSource.ZooKeeper) continue;
                    save(name, settings, client);
                    DefaultLogger.info(String.format("Saved fileSystem settings. [name=%s]", name));
                }
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }

    private void save(String name,
                      FileSystemSettings settings,
                      CuratorFramework client) throws Exception {
        settings.setSource(ESettingsSource.ZooKeeper);
        String path = new PathUtils.ZkPathBuilder(zkBasePath)
                .withPath(name)
                .build();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(path);
        }
        byte[] data = JSONUtils.asBytes(settings);
        client.setData().forPath(path, data);
    }

    private FileSystemSettings read(String path,
                                    CuratorFramework client) throws Exception {
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                FileSystemSettings settings = JSONUtils.read(data, FileSystemSettings.class);
                settings.setSource(ESettingsSource.ZooKeeper);
                if (Strings.isNullOrEmpty(settings.getZkConnection())) {
                    settings.setZkConnection(this.settings.getZkConnection());
                }
                if (Strings.isNullOrEmpty(settings.getZkPath())) {
                    settings.setZkPath(this.zkBasePath);
                }
                return settings;
            }
        }
        return null;
    }


    @Override
    public void close() throws IOException {
        if (state.isRunning()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        if (!syncers.isEmpty()) {
            for (String name : syncers.keySet()) {
                syncers.get(name).close();
                try {
                    Thread t = syncerThreads.get(name);
                    if (t != null) {
                        t.join();
                    } else {
                        throw new Exception(String.format("Syncer thread not found. [name=%s]", name));
                    }
                } catch (Exception ex) {
                    DefaultLogger.stacktrace(ex);
                    DefaultLogger.error(ex.getLocalizedMessage());
                }
            }
            syncers.clear();
            syncerThreads.clear();
        }
        if (!fileSystems.isEmpty()) {
            for (String name : fileSystems.keySet()) {
                fileSystems.get(name).close();
            }
            fileSystems.clear();
        }
    }
}
