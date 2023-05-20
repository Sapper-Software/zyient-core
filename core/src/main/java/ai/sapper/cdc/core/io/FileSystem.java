package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
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
import org.apache.commons.io.FileUtils;
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
    private Thread cleaner;
    private FileSystemConfigReader configReader;
    private String id;

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
        settings = (FileSystemSettings) configReader.settings();
        setup();
    }

    private void setup() throws Exception {
        zkConnection = env.connectionManager()
                .getConnection(settings.getZkConnection(),
                        ZookeeperConnection.class);
        if (zkConnection == null) {
            throw new Exception(String.format("ZooKeeper connection not found. [name=%s]", settings.getZkConnection()));
        }
        if (!zkConnection.isConnected()) {
            zkConnection.connect();
        }
        ModuleInstance instance = env.moduleInstance();
        id = String.format("%s/%s/%s", settings.getName(), instance.getName(), instance.getInstanceId());
        zkPath = new PathUtils.ZkPathBuilder(settings.getZkPath())
                .withPath(settings.getName())
                .build();
        Preconditions.checkState(settings.getContainers() != null && !settings.getContainers().isEmpty());
        domainMap = new FSDomainMap(settings.getDefaultContainer(), settings.getContainers());
        CuratorFramework client = zkConnection.client();
        if (client.checkExists().forPath(zkPath) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zkPath);
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
        }
    }

    protected FileSystem postInit() throws IOException {
        if (settings.isCleanTmp()) {
            cleaner = new Thread(new TmpCleaner(this, tmpDir, settings.getTempTTL()), "TMP-CLEANER-THREAD");
            cleaner.start();
        }
        state.setState(Connection.EConnectionState.Connected);
        return this;
    }

    public DistributedLock getLock(@NonNull Inode inode,
                                   @NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isConnected());
        String zp = inode.getZkPath();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(inode.getName(), zp, zkConnection, settings.getLockTimeout());
    }

    public DistributedLock getLock(@NonNull Inode inode) throws Exception {
        Preconditions.checkState(state.isConnected());
        CuratorFramework client = zkConnection.client();
        return getLock(inode, client);
    }

    protected DistributedLock getDomainLock(@NonNull String domain,
                                            @NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isConnected());
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(domain)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(domain, zp, zkConnection, settings.getLockTimeout());
    }

    private DistributedLock getRootLock(@NonNull CuratorFramework client) throws Exception {
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(settings.getName(), zp, zkConnection, settings.getLockTimeout());
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
            DirectoryInode di = new DirectoryInode(container.getDomain(), "root");
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
        String[] parts = fpath.split("/");
        CuratorFramework client = zkConnection.client();
        return createInode(dnode, path, type, parts, 0, client);
    }

    public Inode updateInode(@NonNull Inode inode,
                             @NonNull PathInfo path) throws IOException {
        Preconditions.checkState(state.isConnected());
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
            try (DistributedLock lock = getLock(current, client)) {
                lock.lock();
                try {
                    client.setData().forPath(inode.getZkPath(), JSONUtils.asBytes(inode, inode.getClass()));
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return inode;
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
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return true;
    }

    private Inode createInode(DirectoryInode parent,
                              PathInfo path,
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
                                DirectoryInode di = new DirectoryInode(parent.getDomain(), parts[index]);
                                di.setParent(parent);
                                di.setPath(path.pathConfig());
                                di.setAbsolutePath(path.path());
                                di.setUuid(UUID.randomUUID().toString());
                                di.setCreateTimestamp(System.currentTimeMillis());
                                di.setUpdateTimestamp(System.currentTimeMillis());
                                di.setSynced(true);
                                di.setZkPath(zpath);
                                di.setPathInfo(path);

                                client.setData().forPath(zpath, JSONUtils.asBytes(di, DirectoryInode.class));
                                return di;
                            }
                        } else {
                            client.create().forPath(zpath);
                            FileInode fi = new FileInode(parent.getDomain(), parts[index]);
                            fi.setParent(parent);
                            fi.setPath(path.pathConfig());
                            fi.setAbsolutePath(path.path());
                            fi.setUuid(UUID.randomUUID().toString());
                            fi.setCreateTimestamp(System.currentTimeMillis());
                            fi.setUpdateTimestamp(System.currentTimeMillis());
                            fi.setZkPath(zpath);
                            fi.setPathInfo(path);
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
                            dnode = new DirectoryInode(parent.getDomain(), parts[index]);
                            dnode.setParent(parent);
                            dnode.setPath(path.pathConfig());
                            dnode.setAbsolutePath(path.path());
                            dnode.setUuid(UUID.randomUUID().toString());
                            dnode.setCreateTimestamp(System.currentTimeMillis());
                            dnode.setUpdateTimestamp(System.currentTimeMillis());
                            dnode.setSynced(true);
                            dnode.setZkPath(zpath);
                            dnode.setPathInfo(path);

                            client.setData().forPath(zpath, JSONUtils.asBytes(dnode, DirectoryInode.class));
                        } finally {
                            lock.unlock();
                        }
                    }
                }
                return createInode(dnode, path, type, parts, index + 1, client);
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public abstract Inode get(@NonNull PathInfo path) throws IOException;

    protected Inode getInode(@NonNull PathInfo path) throws IOException {
        return getInode(path.domain(), path.path());
    }

    protected Inode getInode(@NonNull String domain, @NonNull String path) throws IOException {
        Preconditions.checkState(state.isConnected());
        DirectoryInode dnode = domains.get(domain);
        if (dnode == null) {
            throw new IOException(String.format("Domain directory node not found. [domain=%s]", domain));
        }
        try {
            String zpath = getInodeZkPath(dnode, path);
            CuratorFramework client = zkConnection.client();
            if (client.checkExists().forPath(zpath) != null) {
                return getInode(zpath, Inode.class, client);
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
        return updateInode(node, node.getPathInfo());
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
        return updateInode(node, node.getPathInfo());
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
        return updateInode(node, node.getPathInfo());
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


    public abstract boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException;


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
        Inode pi = get(path);
        if (pi != null) return pi.isDirectory();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public boolean isFile(@NonNull PathInfo path) throws IOException {
        Inode pi = get(path);
        if (pi != null) return pi.isFile();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public boolean isArchive(@NonNull PathInfo path) throws IOException {
        Inode pi = get(path);
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
        Inode node = get(path);
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
        Inode node = get(path);
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

    @Override
    public void close() throws IOException {
        try {
            if (state.isConnected())
                state.setState(Connection.EConnectionState.Closed);
            if (cleaner != null) {
                cleaner.join();
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(LOG, "Error closing file system", ex);
        }
    }

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
        FileSystem create(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws Exception;
    }

    public static class TmpCleaner implements Runnable {
        private final long ttl;
        private final File tmpDir;
        private final FileSystem fs;

        public TmpCleaner(@NonNull FileSystem fs,
                          @NonNull File tmpDir,
                          long ttl) {
            Preconditions.checkArgument(tmpDir.exists());
            Preconditions.checkArgument(ttl >= 60 * 1000);
            this.fs = fs;
            this.tmpDir = tmpDir;
            this.ttl = ttl;
        }

        @Override
        public void run() {
            try {
                while (fs.state.isConnected()) {
                    Thread.sleep(ttl);
                    cleanUp();
                }
            } catch (Throwable t) {
                DefaultLogger.stacktrace(t);
                DefaultLogger.error("Cleaner thread terminated with error.", t);
                fs.state.error(t);
            }
        }

        private void cleanUp() throws Exception {
            Collection<File> files = FileUtils.listFiles(tmpDir, null, true);
            if (files != null && !files.isEmpty()) {
                for (File file : files) {
                    cleanUp(file);
                }
            }
        }

        private void cleanUp(File file) throws Exception {
            long mtime = file.lastModified();
            if (System.currentTimeMillis() - mtime > ttl) {
                file.delete();
            }
        }
    }
}
