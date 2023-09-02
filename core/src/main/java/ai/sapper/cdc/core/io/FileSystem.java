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

package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.*;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.io.encryption.EncryptionHandler;
import ai.sapper.cdc.core.io.encryption.EncryptionType;
import ai.sapper.cdc.core.io.impl.PostOperationVisitor;
import ai.sapper.cdc.core.io.indexing.FileSystemIndexer;
import ai.sapper.cdc.core.io.indexing.FileSystemIndexerSettings;
import ai.sapper.cdc.core.io.model.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public abstract class FileSystem implements Closeable {
    protected final Logger LOG = LoggerFactory.getLogger(FileSystem.class);
    public static final int DEFAULT_READ_BLOCK_SIZE = 1024 * 32;

    private ZookeeperConnection zkConnection;
    protected FileSystemSettings settings;
    protected FSDomainMap domainMap;
    private final Connection.ConnectionState state = new Connection.ConnectionState();
    private String zkPath;
    private final Map<String, DirectoryInode> domains = new HashMap<>();
    private BaseEnv<?> env;
    @Getter(AccessLevel.NONE)
    private File tmpDir;
    @Getter(AccessLevel.NONE)
    private Thread cleanerThread;
    @Getter(AccessLevel.NONE)
    private DirectoryCleaner dirCleaner;
    private FileSystemConfigReader configReader;
    private String id;
    private final List<PostOperationVisitor> visitors = new ArrayList<>();
    private FileSystemMetrics metrics;
    private FileSystemIndexer indexer = null;

    protected FileSystem withZkConnection(@NonNull ZookeeperConnection connection) {
        this.zkConnection = connection;
        return this;
    }

    protected FileSystem withZkPath(@NonNull String zkPath) {
        this.zkPath = zkPath;
        return this;
    }

    public FileSystem addVisitor(@NonNull PostOperationVisitor visitor) {
        visitors.add(visitor);
        return this;
    }

    public abstract Class<? extends FileSystemSettings> getSettingsType();

    public abstract FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull BaseEnv<?> env) throws IOException;

    public FileSystem init(@NonNull FileSystemSettings settings,
                           @NonNull BaseEnv<?> env) throws IOException {
        try {
            this.env = env;
            this.settings = settings;
            setup();
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                     @NonNull BaseEnv<?> env,
                     @NonNull FileSystemConfigReader configReader) throws Exception {
        this.env = env;
        this.configReader = configReader;
        configReader.read();
        this.settings = (FileSystemSettings) configReader.settings();
        setup();
    }

    private void setup() throws Exception {
        if (zkConnection == null) {
            zkConnection = env.connectionManager()
                    .getConnection(settings.getZkConnection(),
                            ZookeeperConnection.class);
            if (zkConnection == null) {
                throw new Exception(String.format("ZooKeeper connection not found. [name=%s]", settings.getZkConnection()));
            }
        }
        if (!zkConnection.isConnected()) {
            zkConnection.connect();
        }
        ModuleInstance instance = env.moduleInstance();
        id = String.format("%s/%s/%s", settings.getName(), instance.getName(), instance.getInstanceId());
        if (Strings.isNullOrEmpty(zkPath)) {
            if (Strings.isNullOrEmpty(settings.getZkPath())) {
                throw new Exception(
                        String.format("[%s] ZooKeeper path not specified in configuration.", settings.getName()));
            }
            zkPath = new PathUtils.ZkPathBuilder(settings.getZkPath())
                    .withPath(settings.getName())
                    .build();
        } else {
            zkPath = new PathUtils.ZkPathBuilder(zkPath)
                    .withPath(settings.getName())
                    .build();
        }
        Preconditions.checkState(settings.getContainers() != null && !settings.getContainers().isEmpty());
        domainMap = new FSDomainMap(settings.getDefaultContainer(), settings.getContainers());
        CuratorFramework client = zkConnection.client();
        if (client.checkExists().forPath(zkPath) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zkPath);
            client.setData().forPath(zkPath, null);
        }
        try {
            Collection<Container> domains = domainMap.getDomains();
            try (DistributedLock lock = getRootLock(client)) {
                lock.lock();
                try {
                    for (Container domain : domains) {
                        registerDomain(domain, client);
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            state.error(ex);
            throw new IOException(ex);
        }
        tmpDir = new File(settings.getTempDir());
        if (!tmpDir.exists()) {
            if (!tmpDir.mkdirs()) {
                throw new IOException(
                        String.format("Error create temporary directory. [path=%s]", tmpDir.getAbsolutePath()));
            }
        } else if (!tmpDir.isDirectory()) {
            throw new IOException(
                    String.format("Path is not a directory. [path=%s]", tmpDir.getAbsolutePath()));
        }
        metrics = new FileSystemMetrics(getClass().getSimpleName(), settings.getName(), "FILESYSTEM", env);
        String ip = FileSystemIndexerSettings.path();
        HierarchicalConfiguration<ImmutableNode> config = configReader.config();
        if (ConfigReader.checkIfNodeExists(config, ip)) {
            indexer = new FileSystemIndexer();
            indexer.init(config, env, this);
        }
    }

    protected FileSystem postInit() throws IOException {
        try {
            if (settings.isCleanTmp()) {
                dirCleaner = new DirectoryCleaner(tmpDir.getAbsolutePath(),
                        true,
                        settings.getTempTTL().normalized(),
                        settings.getTempCleanInterval().normalized());
                cleanerThread = new Thread(dirCleaner, "TMP-CLEANER-THREAD");
                cleanerThread.start();
            }
            state.setState(Connection.EConnectionState.Connected);
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected DistributedLock getLock(@NonNull Inode inode,
                                      @NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isConnected());
        String zp = inode.getZkPath();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(inode.getName(), zp, zkConnection, settings.getLockTimeout().normalized());
    }

    public DistributedLock getLock(@NonNull Inode inode) throws Exception {
        Preconditions.checkState(state.isConnected());
        CuratorFramework client = zkConnection.client();
        return getLock(inode, client);
    }

    public DistributedLock getDomainLock(@NonNull String domain,
                                         @NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isConnected());
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(domain)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(domain, zp, zkConnection, settings.getLockTimeout().normalized());
    }

    private DistributedLock getRootLock(@NonNull CuratorFramework client) throws Exception {
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(settings.getName(), zp, zkConnection, settings.getLockTimeout().normalized());
    }

    private void registerDomain(Container container, CuratorFramework client) throws Exception {
        String path = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(container.getDomain())
                .build();
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                DirectoryInode di = JSONUtils.read(data, DirectoryInode.class);
                domains.put(container.getDomain(), di);
            }
        } else {
            client.create().creatingParentContainersIfNeeded().forPath(path);
            DirectoryInode di = new DirectoryInode(container.getDomain(), "/", "root");
            di.setParent(null);
            di.setPath(container.pathInfo(this).pathConfig());
            di.setUuid(UUID.randomUUID().toString());
            di.setCreateTimestamp(System.currentTimeMillis());
            di.setUpdateTimestamp(System.currentTimeMillis());
            di.setSynced(true);
            di.setZkPath(path);
            di.setAbsolutePath(container.getPath());

            client.setData().forPath(path, JSONUtils.asBytes(di, DirectoryInode.class));
            domains.put(container.getDomain(), di);
        }
    }

    public abstract PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException;

    protected Inode createInode(@NonNull InodeType type,
                                @NonNull PathInfo path) throws IOException {
        Preconditions.checkState(state.isConnected());
        DirectoryInode dnode = domains.get(path.domain());
        if (dnode == null) {
            throw new IOException(String.format("Domain directory node not found. [domain=%s]", path.domain()));
        }
        String fpath = path.path().trim();
        if (fpath.startsWith(dnode.getAbsolutePath())) {
            int index = dnode.getAbsolutePath().length();
            fpath = fpath.substring(index);
        }
        if (fpath.startsWith("/")) {
            fpath = fpath.substring(1);
        }
        return createInode(dnode, type, fpath);
    }

    protected Inode createInode(@NonNull DirectoryInode dnode,
                                @NonNull String name,
                                @NonNull InodeType type) throws IOException {
        Preconditions.checkState(state.isConnected());
        PathInfo path = parsePathInfo(dnode, name, type);
        String fpath = path.path().trim();
        if (fpath.startsWith(dnode.getAbsolutePath())) {
            int index = dnode.getAbsolutePath().length();
            fpath = fpath.substring(index);
        }
        if (fpath.startsWith("/")) {
            fpath = fpath.substring(1);
        }
        return createInode(dnode, type, fpath);
    }

    public Inode updateInode(@NonNull Inode inode) throws IOException {
        Preconditions.checkState(state.isConnected());
        PathInfo path = parsePathInfo(inode.getPath());
        Inode current = getInode(path);
        if (current == null) {
            throw new IOException(String.format("Inode not found: [path=%s]", inode.getZkPath()));
        }
        if (current.getZkPath().compareTo(inode.getZkPath()) != 0) {
            throw new IOException(
                    String.format("Invalid Inode: ZK Path mismatch. [expected=%s][actual=%s]",
                            current.getZkPath(), inode.getZkPath()));
        }
        if (current.getUpdateTimestamp() > inode.getUpdateTimestamp()) {
            throw new IOException(String.format("Inode is stale: [path=%s]", inode.getZkPath()));
        }
        inode.setUpdateTimestamp(System.currentTimeMillis());
        CuratorFramework client = zkConnection.client();
        try {
            client.setData().forPath(inode.getZkPath(), JSONUtils.asBytes(inode, inode.getClass()));
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        for (PostOperationVisitor visitor : visitors) {
            visitor.visit(PostOperationVisitor.Operation.Update, inode);
        }
        return inode;
    }

    public Inode updateInodeWithLock(@NonNull Inode inode) throws IOException {
        CuratorFramework client = zkConnection.client();
        try {
            try (DistributedLock lock = getLock(inode, client)) {
                lock.lock();
                try {
                    return updateInode(inode);
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected boolean deleteInode(@NonNull PathInfo path,
                                  boolean recursive) throws IOException {
        Preconditions.checkState(state.isConnected());
        Inode current = getInode(path);
        if (current == null) {
            return false;
        }
        Inode parent = current.getParent();
        if (parent == null) {
            throw new IOException(String.format("Parent node note found. [zkPath=%s]", current.getParentZkPath()));
        }
        CuratorFramework client = zkConnection.client();
        try (DistributedLock lock = getLock(parent, client)) {
            lock.lock();
            try {
                if (recursive)
                    client.delete().deletingChildrenIfNeeded().forPath(current.getZkPath());
                else {
                    client.setData().forPath(current.getZkPath(), null);
                    List<String> children = client.getChildren().forPath(current.getZkPath());
                    for (String zp : children) {
                        if (zp.compareTo(DistributedLock.ZK_PATH_LOCK) == 0) {
                            String p = new PathUtils.ZkPathBuilder(current.getZkPath())
                                    .withPath(zp)
                                    .build();
                            client.delete().deletingChildrenIfNeeded().forPath(p);
                        }
                    }
                    client.delete().forPath(current.getZkPath());
                }
                for (PostOperationVisitor visitor : visitors) {
                    visitor.visit(PostOperationVisitor.Operation.Delete, current);
                }
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return true;
    }

    private Inode createInode(DirectoryInode parent,
                              InodeType type,
                              String fpath) throws IOException {
        String[] parts = fpath.split("/");
        CuratorFramework client = zkConnection.client();
        Inode node = createInode(parent, type, parts, 0, client);
        for (PostOperationVisitor visitor : visitors) {
            visitor.visit(PostOperationVisitor.Operation.Create, node);
        }
        return node;
    }

    private Inode createInode(DirectoryInode parent,
                              InodeType type,
                              String[] parts,
                              int index,
                              CuratorFramework client) throws IOException {
        Preconditions.checkState(state.isConnected());
        try {
            if (index == parts.length - 1) {
                try (DistributedLock lock = getLock(parent, client)) {
                    lock.lock();
                    try {
                        String zpath = new PathUtils.ZkPathBuilder(parent.getZkPath())
                                .withPath(parts[index])
                                .build();
                        if (type == InodeType.Directory) {
                            if (client.checkExists().forPath(zpath) != null) {
                                Inode node = getInode(zpath, Inode.class, client);
                                if (node != null) {
                                    if (!node.isDirectory()) {
                                        throw new IOException(String.format("Path with name already exists. [path=%s]", zpath));
                                    }
                                    return node;
                                } else {
                                    throw new IOException(String.format("Empty path node: [path=%s]", zpath));
                                }
                            } else {
                                client.create().forPath(zpath);
                                String fsp = PathUtils.formatPath(String.format("%s/%s", parent.getFsPath(), parts[index]));
                                PathInfo pi = parsePathInfo(parent, parts[index], InodeType.Directory);
                                DirectoryInode di = new DirectoryInode(parent.getDomain(), fsp, parts[index]);
                                di.setParent(parent);
                                di.setPath(pi.pathConfig());
                                di.setAbsolutePath(pi.path());
                                di.setUuid(pi.uuid());
                                di.setCreateTimestamp(System.currentTimeMillis());
                                di.setUpdateTimestamp(System.currentTimeMillis());
                                di.setSynced(true);
                                di.setZkPath(zpath);
                                di.setPathInfo(pi);

                                client.setData().forPath(zpath, JSONUtils.asBytes(di, DirectoryInode.class));
                                return di;
                            }
                        } else {
                            client.create().forPath(zpath);
                            PathInfo pi = parsePathInfo(parent, parts[index], InodeType.File);
                            String fsp = PathUtils.formatPath(String.format("%s/%s", parent.getFsPath(), parts[index]));
                            FileInode fi = new FileInode(parent.getDomain(), fsp, parts[index]);
                            fi.setParent(parent);
                            fi.setPath(pi.pathConfig());
                            fi.setAbsolutePath(pi.path());
                            fi.setUuid(pi.uuid());
                            fi.setCreateTimestamp(System.currentTimeMillis());
                            fi.setUpdateTimestamp(System.currentTimeMillis());
                            fi.setZkPath(zpath);
                            fi.setPathInfo(pi);
                            fi.setCompressed(settings.isCompressed());
                            fi.getState().setState(EFileState.New);
                            client.setData().forPath(zpath, JSONUtils.asBytes(fi, FileInode.class));
                            return fi;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } else {
                String zpath = new PathUtils.ZkPathBuilder(parent.getZkPath())
                        .withPath(parts[index])
                        .build();
                DirectoryInode dnode = null;
                if (client.checkExists().forPath(zpath) != null) {
                    dnode = getInode(zpath, DirectoryInode.class, client);
                    if (dnode == null) {
                        throw new IOException(String.format("Empty path node: [path=%s]", zpath));
                    }
                } else {
                    try (DistributedLock lock = getLock(parent, client)) {
                        lock.lock();
                        try {
                            client.create().forPath(zpath);
                            PathInfo pi = parsePathInfo(parent, parts[index], InodeType.Directory);
                            String fsp = PathUtils.formatPath(String.format("%s/%s", parent.getFsPath(), parts[index]));
                            dnode = new DirectoryInode(parent.getDomain(), fsp, parts[index]);
                            dnode.setParent(parent);
                            dnode.setPath(pi.pathConfig());
                            dnode.setAbsolutePath(pi.path());
                            dnode.setUuid(pi.uuid());
                            dnode.setCreateTimestamp(System.currentTimeMillis());
                            dnode.setUpdateTimestamp(System.currentTimeMillis());
                            dnode.setSynced(true);
                            dnode.setZkPath(zpath);
                            dnode.setPathInfo(pi);

                            client.setData().forPath(zpath, JSONUtils.asBytes(dnode, DirectoryInode.class));
                        } finally {
                            lock.unlock();
                        }
                    }
                }
                return createInode(dnode, type, parts, index + 1, client);
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public Inode getInode(@NonNull PathInfo path) throws IOException {
        return getInode(path.domain(), path.path());
    }

    public Inode getInode(@NonNull String domain, @NonNull String path) throws IOException {
        Preconditions.checkState(state.isConnected());
        DirectoryInode dnode = domains.get(domain);
        if (dnode == null) {
            throw new IOException(String.format("Domain directory node not found. [domain=%s]", domain));
        }
        try {
            String zpath = getInodeZkPath(dnode, path);
            CuratorFramework client = zkConnection.client();
            if (client.checkExists().forPath(zpath) != null) {
                DefaultLogger.trace(String.format("Path found. [domain=%s][path=%s]", domain, path));
                return getInode(zpath, Inode.class, client);
            } else {
                DefaultLogger.trace(String.format("Path not found. [domain=%s][path=%s]", domain, path));
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public File createTmpDir(@NonNull String path) throws IOException {
        Preconditions.checkState(state.isConnected());
        String tpath = PathUtils.formatPath(String.format("%s/%s", tmpDir.getAbsolutePath(), path));
        File dir = new File(tpath);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(
                        String.format("Failed to create temporary directory. [path=%s]", dir.getAbsolutePath()));
            }
        }
        return dir;
    }

    public File createTmpFile() throws IOException {
        return createTmpFile(null, null);
    }

    public File createTmpFile(String path) throws IOException {
        return createTmpFile(path, null);
    }

    public File createTmpFile(String path, String name) throws IOException {
        Preconditions.checkState(state.isConnected());
        File tdir = tmpDir;
        if (!Strings.isNullOrEmpty(path)) {
            tdir = createTmpDir(path);
        }
        if (Strings.isNullOrEmpty(name)) {
            name = String.format("%s.tmp", UUID.randomUUID().toString());
        }
        String fpath = PathUtils.formatPath(String.format("%s/%s", tdir.getAbsolutePath(), name));
        File file = new File(fpath);
        if (!file.setLastModified(System.currentTimeMillis())) {
            DefaultLogger.warn(LOG, String.format("Failed to touch file. [path=%s]", file.getAbsolutePath()));
        }

        return file;
    }

    protected String getInodeZkPath(@NonNull DirectoryInode mnode, @NonNull String path) {
        if (path.startsWith(mnode.getAbsolutePath())) {
            path = path.substring(mnode.getAbsolutePath().length());
        }
        return new PathUtils.ZkPathBuilder(mnode.getZkPath())
                .withPath(path)
                .build();
    }

    public Inode fileLock(@NonNull FileInode node) throws Exception {
        Preconditions.checkArgument(node.getPathInfo() != null);
        FileInode current = (FileInode) getInode(node.getPathInfo());
        if (current.getLock() == null) {
            FileInodeLock lock = new FileInodeLock(id, settings.getName());
            node.setLock(lock);
        } else {
            if (id.compareTo(current.getLock().getClientId()) != 0) {
                throw new DistributedLock.LockError(
                        String.format("[FS: %s] File already locked. [client ID=%s]",
                                settings.getName(), current.getLock().getClientId()));
            } else {
                node.setLock(current.getLock());
            }
        }
        if (Strings.isNullOrEmpty(node.getLock().getLocalPath())) {
            File temp = createTmpFile(null, node.getName());
            if (temp.exists()) {
                if (!temp.delete()) {
                    DefaultLogger.warn(LOG, String.format("Failed to delete file. [path=%s]", temp.getAbsolutePath()));
                }
            }
            node.getLock().setLocalPath(temp.getAbsolutePath());
        }
        node.getLock().setTimeUpdated(System.currentTimeMillis());
        return updateInode(node);
    }

    public Inode fileUnlock(@NonNull FileInode node) throws Exception {
        Preconditions.checkArgument(node.getPathInfo() != null);
        FileInode current = (FileInode) getInode(node.getPathInfo());
        if (current.getLock() == null) {
            throw new Exception(
                    String.format("[FS: %s] File not locked. [domain=%s][path=%s]",
                            settings.getName(), node.getDomain(), node.getAbsolutePath()));
        }
        if (id.compareTo(current.getLock().getClientId()) != 0) {
            throw new DistributedLock.LockError(
                    String.format("[FS: %s] File not locked by current file system. [client ID=%s]",
                            settings.getName(), current.getLock().getClientId()));
        }
        node.setLock(null);
        return updateInode(node);
    }

    public Inode fileUpdateLock(@NonNull FileInode node) throws Exception {
        Preconditions.checkArgument(node.getPathInfo() != null);
        FileInode current = (FileInode) getInode(node.getPathInfo());
        if (current.getLock() == null) {
            throw new Exception(
                    String.format("[FS: %s] File not locked. [domain=%s][path=%s]",
                            settings.getName(), node.getDomain(), node.getAbsolutePath()));
        }
        if (id.compareTo(current.getLock().getClientId()) != 0) {
            throw new DistributedLock.LockError(
                    String.format("[FS: %s] File not locked by current file system. [client ID=%s]",
                            settings.getName(), current.getLock().getClientId()));
        }
        node.setLock(current.getLock());
        node.getLock().setTimeUpdated(System.currentTimeMillis());
        return updateInode(node);
    }

    public boolean isFileLocked(@NonNull FileInode node) throws Exception {
        Preconditions.checkArgument(node.getPathInfo() != null);
        FileInode current = (FileInode) getInode(node.getPathInfo());
        if (current.getLock() == null) {
            return false;
        }
        if (!current.getState().markedForUpdate()) {
            return false;
        }
        return id.compareTo(current.getLock().getClientId()) == 0;
    }

    protected abstract String getAbsolutePath(@NonNull String path,
                                              @NonNull String domain) throws IOException;

    public abstract DirectoryInode mkdir(@NonNull DirectoryInode path, @NonNull String name) throws IOException;

    public abstract DirectoryInode mkdirs(@NonNull String domain, @NonNull String path) throws IOException;

    public abstract FileInode create(@NonNull String domain, @NonNull String path) throws IOException;

    public abstract FileInode create(@NonNull PathInfo pathInfo) throws IOException;

    public abstract FileInode create(@NonNull DirectoryInode dir, @NonNull String name) throws IOException;

    public abstract boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException;

    protected abstract PathInfo parsePathInfo(@NonNull DirectoryInode parent,
                                              @NonNull String path,
                                              @NonNull InodeType type) throws IOException;

    public boolean delete(@NonNull PathInfo path) throws IOException {
        return delete(path, false);
    }

    public List<Inode> list(@NonNull PathInfo path, boolean recursive) throws IOException {
        return list(path.domain(), path.path(), recursive);
    }

    public List<Inode> list(@NonNull String module,
                            @NonNull String path,
                            boolean recursive) throws IOException {
        Preconditions.checkState(state.isConnected());
        Inode root = getInode(module, path);
        if (root != null) {
            if (root.isDirectory()) {
                CuratorFramework client = zkConnection.client();
                try {
                    List<Inode> nodes = new ArrayList<>();
                    list((DirectoryInode) root, recursive, nodes, client);
                    if (!nodes.isEmpty()) return nodes;
                } catch (Exception ex) {
                    throw new IOException(ex);
                }
            }
        }
        return null;
    }

    private void list(DirectoryInode parent,
                      boolean recursive,
                      List<Inode> inodes,
                      CuratorFramework client) throws Exception {
        List<String> paths = client.getChildren().forPath(parent.getZkPath());
        if (paths != null && !paths.isEmpty()) {
            for (String path : paths) {
                String cpath = new PathUtils.ZkPathBuilder(parent.getZkPath())
                        .withPath(path)
                        .build();
                Inode cnode = getInode(cpath, Inode.class, client);
                if (cnode != null) {
                    if (cnode.isFile() || cnode.isArchive()) {
                        inodes.add(cnode);
                    } else if (cnode.isDirectory()) {
                        if (recursive) {
                            list((DirectoryInode) cnode, true, inodes, client);
                        } else {
                            inodes.add(cnode);
                        }
                    }
                }
            }
        }
    }

    private <T extends Inode> T getInode(String path,
                                         Class<T> type,
                                         CuratorFramework client) throws Exception {
        byte[] data = client.getData().forPath(path);
        if (data != null && data.length > 0) {
            T node = JSONUtils.read(data, type);
            node.setPathInfo(parsePathInfo(node.getPath()));
            if (!Strings.isNullOrEmpty(node.getParentZkPath())) {
                Inode parent = getParent(node.getParentZkPath(), client);
                if (parent == null) {
                    throw new Exception(String.format("Parent node not found. [path=%s]", node.getParentZkPath()));
                }
                node.setParent(parent);
            }
            return node;
        }
        return null;
    }

    private Inode getParent(String path, CuratorFramework client) throws Exception {
        byte[] data = client.getData().forPath(path);
        if (data != null && data.length > 0) {
            Inode node = JSONUtils.read(data, Inode.class);
            node.setPathInfo(parsePathInfo(node.getPath()));
            return node;
        }
        return null;
    }

    public List<Inode> find(@NonNull PathInfo path,
                            String dirQuery,
                            @NonNull String fileQuery) throws IOException {
        Preconditions.checkState(state.isConnected());
        List<Inode> inodes = list(path, true);
        List<Inode> filtered = new ArrayList<>();
        if (inodes != null && !inodes.isEmpty()) {
            Pattern dp = null;
            if (!Strings.isNullOrEmpty(dirQuery)) {
                dp = Pattern.compile(dirQuery);
            }
            Pattern fp = Pattern.compile(fileQuery);
            for (Inode node : inodes) {
                if (node.isFile()) {
                    if (dp != null) {
                        Matcher dm = dp.matcher(node.getParent().getAbsolutePath());
                        if (!dm.matches()) {
                            continue;
                        }
                    }
                    Matcher fm = fp.matcher(node.getName());
                    if (fm.matches()) {
                        filtered.add(node);
                    }
                }
            }
        }
        if (!filtered.isEmpty()) return filtered;
        return null;
    }

    public abstract boolean exists(@NonNull PathInfo path) throws IOException;

    public boolean isDirectory(@NonNull PathInfo path) throws IOException {
        Inode pi = getInode(path);
        if (pi != null) return pi.isDirectory();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public boolean isFile(@NonNull PathInfo path) throws IOException {
        Inode pi = getInode(path);
        if (pi != null) return pi.isFile();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public boolean isArchive(@NonNull PathInfo path) throws IOException {
        Inode pi = getInode(path);
        if (pi != null) return pi.isArchive();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public final Writer writer(@NonNull PathInfo path) throws IOException {
        return writer(path, false);
    }

    public final Writer writer(@NonNull PathInfo path,
                               boolean overwrite) throws IOException {
        Inode node = getInode(path);
        if (node == null) {
            node = createInode(InodeType.File, path);
        }
        return writer((FileInode) node, overwrite);
    }

    public final Writer writer(@NonNull FileInode inode,
                               boolean overwrite) throws IOException {
        Preconditions.checkState(state.isConnected());
        return getWriter(inode, overwrite);
    }

    public final Writer writer(@NonNull FileInode inode) throws IOException {
        return writer(inode, false);
    }

    public final Reader reader(@NonNull PathInfo path) throws IOException {
        Inode node = getInode(path);
        if (node == null) {
            throw new IOException(String.format("File does not exist. [path=%s]", path));
        }
        if (!(node instanceof FileInode)) {
            throw new IOException(String.format("Path is not a file. [path=%s]", path));
        }
        return reader((FileInode) node);
    }

    public final Reader reader(@NonNull FileInode inode) throws IOException {
        Preconditions.checkState(state.isConnected());
        return getReader(inode);
    }

    protected boolean checkInodeAvailable(@NonNull FileInode inode, long timeout) throws IOException {
        long start = System.currentTimeMillis();
        while (true) {
            long d = System.currentTimeMillis() - start;
            if (d > timeout) {
                break;
            }
            FileInode current = (FileInode) getInode(inode.getPathInfo());
            FileState state = current.getState();
            if (state.hasError()) {
                throw new IOException(state.getError());
            }
            if (state.available()) {
                return true;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                // Do nothing...
            }
        }
        return false;
    }

    public FileInode setEncryption(@NonNull PathInfo path,
                                   @NonNull EncryptionType type,
                                   String password) throws IOException {
        FileInode inode = (FileInode) getInode(path);
        if (inode == null) {
            throw new IOException(String.format("File not found. [path=%s]", path.path()));
        }
        if (type != EncryptionType.None) {
            if (Strings.isNullOrEmpty(password)) {
                throw new IOException("Password not specified.");
            }
            if (Strings.isNullOrEmpty(settings.getEncryptionKey())) {
                throw new IOException("Encryption key not specified in settings.");
            }
            Encrypted e = new Encrypted();
            try {
                String passKey = env.keyStore().read(settings.getEncryptionKey());
                if (Strings.isNullOrEmpty(passKey)) {
                    throw new IOException(
                            String.format("Encryption key not found. [key=%s]", settings.getEncryptionKey()));
                }
                String key = CypherUtils.encryptAsString(password, passKey, getClass().getCanonicalName());
                e.setKey(key);
                inode.setEncrypted(e);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        } else {
            inode.setEncryption(EncryptionType.None);
            inode.setEncrypted(null);
        }
        return (FileInode) updateInode(inode);
    }

    public EncryptionHandler getEncryptionHandler(@NonNull PathInfo path) throws IOException {
        FileInode inode = (FileInode) getInode(path);
        if (inode == null) {
            throw new IOException(String.format("File not found. [path=%s]", path.path()));
        }
        return getEncryptionHandler(inode);
    }

    public EncryptionHandler getEncryptionHandler(@NonNull FileInode inode) throws IOException {
        if (inode.getEncryption() != EncryptionType.None) {

        }
        return null;
    }

    public File compress(@NonNull File file) throws IOException {
        Preconditions.checkArgument(file.exists());
        byte[] data = Files.readAllBytes(Paths.get(file.toURI()));
        if (data.length > 0) {
            byte[] compressed = Snappy.compress(data);
            File outf = createTmpFile();
            Files.write(Paths.get(outf.toURI()), compressed);
            return outf;
        }
        return null;
    }

    public File decompress(@NonNull File file) throws IOException {
        Preconditions.checkArgument(file.exists());
        byte[] data = Files.readAllBytes(Paths.get(file.toURI()));
        if (data.length > 0) {
            byte[] uncompressed = Snappy.uncompress(data);
            File outf = createTmpFile();
            Files.write(Paths.get(outf.toURI()), uncompressed);
            return outf;
        }
        return null;
    }

    protected abstract Reader getReader(@NonNull FileInode inode) throws IOException;

    protected abstract Writer getWriter(@NonNull FileInode inode,
                                        boolean overwrite) throws IOException;

    public FileInode copy(@NonNull FileInode source,
                          @NonNull PathInfo target) throws IOException {
        return copy(source, target, false);
    }

    public FileInode copy(@NonNull FileInode source,
                          @NonNull PathInfo target,
                          boolean overwrite) throws IOException {
        FileInode tf = (FileInode) getInode(target);
        if (!overwrite && tf != null) {
            throw new IOException(String.format("Target file already exists. [path=%s]", target.path()));
        }
        if (tf == null) {
            tf = create(target);
        }
        if (tf.getPathInfo() == null) {
            tf.setPathInfo(parsePathInfo(tf.getPath()));
        }
        try {
            doCopy(source, tf);
            return (FileInode) updateInode(tf);
        } catch (Throwable t) {
            delete(tf.getPathInfo());
            throw new IOException(t);
        }
    }

    protected abstract void doCopy(@NonNull FileInode source, @NonNull FileInode target) throws IOException;

    public FileInode move(@NonNull FileInode source,
                          @NonNull PathInfo target,
                          boolean overwrite) throws IOException {
        FileInode tf = (FileInode) getInode(target);
        if (!overwrite && tf != null) {
            throw new IOException(String.format("Target file already exists. [path=%s]", target.path()));
        }
        if (tf == null) {
            tf = create(target);
        }
        if (tf.getPathInfo() == null) {
            tf.setPathInfo(parsePathInfo(tf.getPath()));
        }
        try {
            doRename(source, tf);
            delete(source.getPathInfo(), true);
            return (FileInode) updateInode(tf);
        } catch (Throwable t) {
            delete(tf.getPathInfo());
            throw new IOException(t);
        }
    }

    public FileInode rename(@NonNull FileInode source,
                            @NonNull String name,
                            boolean overwrite) throws IOException {
        PathInfo target = renameFile(source, name);
        return move(source, target, overwrite);
    }

    protected abstract PathInfo renameFile(@NonNull FileInode source, @NonNull String name) throws IOException;

    protected abstract void doRename(@NonNull FileInode source, @NonNull FileInode target) throws IOException;

    @Override
    public void close() throws IOException {
        try {
            if (state.isConnected())
                state.setState(Connection.EConnectionState.Closed);
            if (cleanerThread != null) {
                dirCleaner.stop();
                cleanerThread.join();
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(LOG, "Error closing file system", ex);
        }
    }

    public FileInode upload(@NonNull File source,
                            @NonNull FileInode path) throws IOException {
        return upload(source, path, true);
    }

    public abstract FileInode upload(@NonNull File source,
                                     @NonNull FileInode path,
                                     boolean clearLock) throws IOException;

    public abstract File download(@NonNull FileInode inode, long timeout) throws IOException;

    @Getter
    @Accessors(fluent = true)
    public static abstract class FileSystemConfigReader extends ConfigReader {
        private final Class<? extends Container> containerType;

        public FileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                      @NonNull String path,
                                      @NonNull Class<? extends FileSystemSettings> type,
                                      @NonNull Class<? extends Container> containerType) {
            super(config, path, type);
            this.containerType = containerType;
        }

        public FileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                      @NonNull Class<? extends FileSystemSettings> type,
                                      @NonNull Class<? extends Container> containerType) {
            super(config, type);
            this.containerType = containerType;
        }

        @Override
        public void read() throws ConfigurationException {
            super.read();
            ContainerConfigReader reader = new ContainerConfigReader(config(), containerType);
            reader.read();
            FileSystemSettings settings = (FileSystemSettings) settings();
            settings.setDefaultContainer(reader.defaultContainer());
            settings.setContainers(reader.containers());
        }
    }

    public interface FileSystemMocker {
        FileSystem create(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                          @NonNull BaseEnv<?> env) throws Exception;
    }
}
