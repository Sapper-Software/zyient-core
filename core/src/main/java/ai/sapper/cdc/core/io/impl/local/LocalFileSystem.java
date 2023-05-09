package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.*;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class LocalFileSystem extends CDCFileSystem {
    private final Class<? extends FileSystemSettings> settingsType;

    public LocalFileSystem() {
        settingsType = FileSystemSettings.class;
    }

    public LocalFileSystem(@NonNull Class<? extends FileSystemSettings> settingsType) {
        this.settingsType = settingsType;
    }

    @Override
    public FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws IOException {
        try {
            super.init(config, env, new LocalFileSystemConfigReader(config));
            return postInit();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(LOG, "Error initializing Local FileSystem.", t);
            state().error(t);
            throw new IOException(t);
        }
    }

    @Override
    public PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException {
        return new LocalPathInfo(this, values);
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Inode get(@NonNull String path, String domain) throws IOException {
        path = PathUtils.formatPath(path);
        if (!Strings.isNullOrEmpty(domain)) {
            Container container = domainMap.get(domain);
            if (!(container instanceof LocalContainer)) {
                throw new IOException(String.format("Mapped container not found. [domain=%s]", domain));
            }
            String pp = ((LocalContainer) container).getPath();
            if (!path.startsWith(pp)) {
                path = PathUtils.formatPath(String.format("%s/%s", pp, path));
            }
        }
        PathInfo pi = new LocalPathInfo(this, path, domain);
        return getInode(pi);
    }

    @Override
    protected String getAbsolutePath(@NonNull String path,
                                     @NonNull String domain) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        Container container = domainMap.get(domain);
        if (!(container instanceof LocalContainer)) {
            throw new IOException(String.format("Mapped container not found. [domain=%s]", domain));
        }
        String pp = ((LocalContainer) container).getPath();

        return PathUtils.formatPath(String.format("/%s/%s/%s",
                pp, domain, path));
    }

    @Override
    public DirectoryInode mkdir(@NonNull DirectoryInode parent,
                                @NonNull String name) throws IOException {
        String path = PathUtils.formatPath(String.format("%s/%s", parent.getAbsolutePath(), name));
        LocalPathInfo pd = (LocalPathInfo) parent.getPathInfo();
        if (pd == null) {
            pd = new LocalPathInfo(this, parent);
            parent.setPathInfo(pd);
        }
        if (!pd.file().exists()) {
            throw new IOException(String.format("Parent directory not found. [path=%s]", pd.file().getAbsolutePath()));
        }
        LocalPathInfo pi = new LocalPathInfo(this, path, pd.domain());
        Inode node = createInode(InodeType.Directory, pi);
        if (!pi.file().exists()) {
            if (!pi.file().mkdir()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", pi.file().getAbsolutePath()));
            }
        }
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) node;
    }

    @Override
    public DirectoryInode mkdirs(@NonNull String module, @NonNull String path) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        path = getAbsolutePath(path, module);
        LocalPathInfo pi = new LocalPathInfo(this, path, module);
        Inode node = createInode(InodeType.Directory, pi);
        if (!pi.file().exists()) {
            if (!pi.file().mkdirs()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", pi.file().getAbsolutePath()));
            }
        }
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) node;
    }

    @Override
    public FileInode create(@NonNull String module, @NonNull String path) throws IOException {
        LocalPathInfo pi = new LocalPathInfo(this, path, module);
        return create(pi);
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Preconditions.checkArgument(pathInfo instanceof LocalPathInfo);
        LocalPathInfo pi = (LocalPathInfo) pathInfo;
        Inode node = createInode(InodeType.File, pi);
        File dir = pi.file().getParentFile();
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException(
                        String.format("Error creating directory. [path=%s]", dir.getAbsolutePath()));
            }
        }
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (FileInode) node;
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public boolean delete(@NonNull PathInfo path,
                          boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof LocalPathInfo);
        if (deleteInode(path, recursive)) {
            if (path.exists()) {
                if (((LocalPathInfo) path).file().isDirectory() && recursive) {
                    FileUtils.deleteDirectory(((LocalPathInfo) path).file());
                    return path.exists();
                } else {
                    return ((LocalPathInfo) path).file().delete();
                }
            }
        }
        return false;
    }

    @Override
    public boolean exists(@NonNull String path, String domain) throws IOException {
        Inode node = get(path, domain);
        if (node != null) {
            LocalPathInfo pi = (LocalPathInfo) parsePathInfo(node.getPath());
            return pi.exists();
        }
        return false;
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) inode.getPathInfo();
        if (pi == null) {
            pi = new LocalPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        if (!pi.exists()) {
            throw new IOException(String.format("Local file not found. [path=%s]", inode.getAbsolutePath()));
        }
        return new LocalReader(inode, this).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode,
                               boolean overwrite) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) inode.getPathInfo();
        if (pi == null) {
            pi = new LocalPathInfo(this, inode);
            inode.setPathInfo(pi);
        }
        return new LocalWriter(inode, this, overwrite).open();
    }

    public static class LocalFileSystemConfigReader extends FileSystemConfigReader {
        public static final String __CONFIG_PATH = "local";

        public LocalFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                           @NonNull String path,
                                           @NonNull Class<? extends FileSystemSettings> type,
                                           @NonNull Class<? extends Container> containerType) {
            super(config, path, type, containerType);
        }

        public LocalFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, FileSystemSettings.class, LocalContainer.class);
        }
    }
}
