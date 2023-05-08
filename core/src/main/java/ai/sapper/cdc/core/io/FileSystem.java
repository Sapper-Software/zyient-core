package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.io.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public abstract class FileSystem<T extends FileSystem.FileSystemSettings> implements Closeable {
    protected final Logger LOG = LoggerFactory.getLogger(FileSystem.class);

    private FileSystemHelper helper = null;
    private ZookeeperConnection zkConnection;
    protected ConfigReader configReader;
    protected T settings;
    protected FSDomainMap domainMap;
    private final Connection.ConnectionState state = new Connection.ConnectionState();
    private String zkPath;
    private final Map<String, DirectoryInode> domains = new HashMap<>();
    private BaseEnv<?> env;
    @Getter(AccessLevel.NONE)
    private File tmpDir;
    @Getter(AccessLevel.NONE)
    private Thread cleaner;

    public abstract FileSystem<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                       String pathPrefix,
                                       @NonNull BaseEnv<?> env) throws IOException;

    @SuppressWarnings("unchecked")
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                     @NonNull String pathPrefix,
                     @NonNull BaseEnv<?> env,
                     @NonNull Class<T> settingsType) throws Exception {
        Preconditions.checkArgument(env.state().isAvailable());

        this.env = env;
        configReader = new ConfigReader(config, pathPrefix, settingsType);
        configReader.read();
        settings = (T) configReader.settings();
        zkConnection = env.connectionManager()
                .getConnection(settings.zkConnection(),
                        ZookeeperConnection.class);
        if (zkConnection == null) {
            throw new Exception(String.format("ZooKeeper connection not found. [name=%s]", settings.zkConnection()));
        }
        if (!zkConnection.isConnected()) {
            zkConnection.connect();
        }
        zkPath = new PathUtils.ZkPathBuilder(settings.zkPath())
                .withPath(settings.name())
                .build();
        CuratorFramework client = zkConnection.client();
        if (client.checkExists().forPath(zkPath) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zkPath);
        }
        domainMap = new FSDomainMap(settings.defaultDomain(), settings.domains());

        Collection<String> domains = domainMap.getDomains();
        try (DistributedLock lock = getRootLock(client)) {
            lock.lock();
            try {
                for (String domain : domains) {
                    registerDomain(domain, client);
                }
            } finally {
                lock.unlock();
            }
        }
        tmpDir = new File(settings.tempDir());
        if (!tmpDir.exists()) {
            if (!tmpDir.mkdirs()) {
                throw new IOException(
                        String.format("Error create temporary directory. [path=%s]", tmpDir.getAbsolutePath()));
            }
        }
    }

    protected FileSystem<T> postInit() throws IOException {
        if (settings.cleanTmp()) {
            cleaner = new Thread(new TmpCleaner(this, tmpDir, settings.tempTTL()), "TMP-CLEANER-THREAD");
            cleaner.start();
        }
        return this;
    }

    public DistributedLock getLock(@NonNull Inode inode,
                                   @NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isConnected());
        String zp = inode.getZkPath();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(zp, zkConnection, settings.lockTimeout());
    }

    protected DistributedLock getDomainLock(@NonNull String domain,
                                            @NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isConnected());
        String target = domainMap.get(domain);
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(target)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(zp, zkConnection, settings.lockTimeout());
    }

    private DistributedLock getRootLock(@NonNull CuratorFramework client) throws Exception {
        Preconditions.checkState(state.isConnected());
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(zp, zkConnection, settings.lockTimeout());
    }

    private void registerDomain(String name, CuratorFramework client) throws Exception {
        String path = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(name)
                .build();
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                DirectoryInode di = JSONUtils.read(data, DirectoryInode.class);
                domains.put(name, di);
            }
        } else {
            client.create().creatingParentContainersIfNeeded().forPath(path);
            DirectoryInode di = new DirectoryInode(name);
            di.setParent(null);
            di.setPath(null);
            di.setUuid(UUID.randomUUID().toString());
            di.setCreateTimestamp(System.currentTimeMillis());
            di.setUpdateTimestamp(System.currentTimeMillis());
            di.setSynced(true);
            di.setZkPath(path);

            client.setData().forPath(path, JSONUtils.asBytes(di, DirectoryInode.class));
            domains.put(name, di);
        }
    }

    public abstract PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException;

    protected Inode createInode(@NonNull InodeType type,
                                @NonNull PathInfo path) throws IOException {
        Preconditions.checkState(state.isConnected());
        String target = domainMap.get(path.domain());
        DirectoryInode dnode = domains.get(target);
        if (dnode == null) {
            throw new IOException(String.format("Domain directory node not found. [domain=%s]", target));
        }
        String fpath = path.path();
        String[] parts = fpath.split("/");
        CuratorFramework client = zkConnection.client();
        return createInode(dnode, path, type, parts, 0, client);
    }

    protected Inode updateInode(@NonNull Inode inode,
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
        try (DistributedLock lock = getLock(current, client)) {
            lock.lock();
            try {
                client.setData().forPath(inode.getZkPath(), JSONUtils.asBytes(inode, inode.getClass()));
            } finally {
                lock.unlock();
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
        CuratorFramework client = zkConnection.client();
        try (DistributedLock lock = getLock(current, client)) {
            lock.lock();
            try {
                if (recursive)
                    client.delete().deletingChildrenIfNeeded().forPath(current.getZkPath());
                else
                    client.delete().forPath(current.getZkPath());
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
                                DirectoryInode di = new DirectoryInode(parts[index]);
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
                            if (client.checkExists().forPath(zpath) != null) {
                                throw new IOException(String.format("File already registered: [path=%s]", zpath));
                            }
                            FileInode fi = new FileInode(parts[index]);
                            fi.setParent(parent);
                            fi.setPath(path.pathConfig());
                            fi.setAbsolutePath(path.path());
                            fi.setUuid(UUID.randomUUID().toString());
                            fi.setCreateTimestamp(System.currentTimeMillis());
                            fi.setUpdateTimestamp(System.currentTimeMillis());
                            fi.setZkPath(zpath);
                            fi.setPathInfo(path);

                            client.setData().forPath(zpath, JSONUtils.asBytes(fi, DirectoryInode.class));
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
                            dnode = new DirectoryInode(parts[index]);
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

    public abstract Inode get(@NonNull String path, String domain) throws IOException;

    protected Inode getInode(@NonNull PathInfo path) throws IOException {
        return getInode(path.domain(), path.path());
    }

    protected Inode getInode(@NonNull String module, @NonNull String path) throws IOException {
        Preconditions.checkState(state.isConnected());
        String target = domainMap.get(module);
        DirectoryInode dnode = domains.get(target);
        if (dnode == null) {
            throw new IOException(String.format("Domain directory node not found. [domain=%s]", target));
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
        return new File(fpath);
    }

    protected String getInodeZkPath(DirectoryInode mnode, String path) {
        return new PathUtils.ZkPathBuilder(mnode.getZkPath())
                .withPath(path)
                .build();
    }

    protected abstract String getAbsolutePath(@NonNull String path,
                                              @NonNull String module);

    public abstract DirectoryInode mkdir(@NonNull DirectoryInode path, @NonNull String name) throws IOException;

    public abstract DirectoryInode mkdirs(@NonNull String module, @NonNull String path) throws IOException;

    public abstract FileInode create(@NonNull String module, @NonNull String path) throws IOException;

    public abstract FileInode create(@NonNull PathInfo pathInfo) throws IOException;

    public abstract FileInode upload(@NonNull File source, @NonNull DirectoryInode directory) throws IOException;

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

    public abstract boolean exists(@NonNull String path, String domain) throws IOException;

    public boolean isDirectory(@NonNull String path, String domain) throws IOException {
        Inode pi = get(path, domain);
        if (pi != null) return pi.isDirectory();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public boolean isFile(@NonNull String path, String domain) throws IOException {
        Inode pi = get(path, domain);
        if (pi != null) return pi.isFile();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public boolean isArchive(@NonNull String path, String domain) throws IOException {
        Inode pi = get(path, domain);
        if (pi != null) return pi.isArchive();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public final Writer writer(@NonNull Inode inode,
                               boolean createDir,
                               boolean overwrite) throws IOException {
        Preconditions.checkState(state.isConnected());
        return getWriter(inode, createDir, overwrite);
    }

    public final Writer writer(@NonNull Inode inode,
                               boolean overwrite) throws IOException {
        return writer(inode, false, overwrite);
    }

    public final Writer writer(@NonNull Inode inode) throws IOException {
        return writer(inode, false, false);
    }

    public final Reader reader(@NonNull Inode inode) throws IOException {
        Preconditions.checkState(state.isConnected());
        return getReader(inode);
    }

    protected abstract Reader getReader(@NonNull Inode inode) throws IOException;

    protected abstract Writer getWriter(@NonNull Inode inode,
                                        boolean createDir,
                                        boolean overwrite) throws IOException;

    @Override
    public void close() throws IOException {
        try {
            if (state.isConnected())
                state.state(Connection.EConnectionState.Closed);
            if (cleaner != null) {
                cleaner.join();
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(LOG, "Error closing file system", ex);
        }
    }

    @Getter
    @Setter
    public static class FileSystemSettings extends Settings {
        public static final String TEMP_PATH = String.format("%s/zyient/cdc",
                System.getProperty("java.io.tmpdir"));

        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_DEFAULT_DOMAIN = "domain";
        public static final String CONFIG_ROOT = "root";
        public static final String CONFIG_TEMP_FOLDER = "tmp.path";
        public static final String CONFIG_TEMP_TTL = "tmp.ttl";
        public static final String CONFIG_TEMP_CLEAN = "tmp.clean";
        public static final String CONFIG_ZK_CONNECTION = "zk.connection";
        public static final String CONFIG_ZK_PATH = "zk.path";
        public static final String CONFIG_ZK_LOCK_TIMEOUT = "zk.lockTimeout";
        public static final int LOCK_TIMEOUT = 60 * 1000;

        @Config(name = CONFIG_NAME)
        private String name;
        @Config(name = CONFIG_DEFAULT_DOMAIN)
        private String defaultDomain;
        @Config(name = CONFIG_ZK_CONNECTION)
        private String zkConnection;
        @Config(name = CONFIG_ZK_PATH)
        private String zkPath;
        @Config(name = CONFIG_ROOT)
        private String pathPrefix;
        @Config(name = CONFIG_TEMP_FOLDER, required = false)
        private String tempDir = TEMP_PATH;
        @Config(name = CONFIG_TEMP_CLEAN, required = false, type = Boolean.class)
        private boolean cleanTmp = true;
        @Config(name = CONFIG_TEMP_TTL, required = false, type = Long.class)
        private long tempTTL = 15 * 60 * 1000;
        @Config(name = "domains", required = false, type = Map.class)
        private Map<String, String> domains;
        @Config(name = CONFIG_ZK_LOCK_TIMEOUT, required = false, type = Integer.class)
        private int lockTimeout = LOCK_TIMEOUT;
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
                DefaultLogger.LOGGER.error("Cleaner thread terminated with error.", t);
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
