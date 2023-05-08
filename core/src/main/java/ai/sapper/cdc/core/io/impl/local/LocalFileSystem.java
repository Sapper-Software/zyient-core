package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.*;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class LocalFileSystem<T extends FileSystem.FileSystemSettings> extends CDCFileSystem {
    public static final String __CONFIG_PATH = "local";

    private final Class<T> settingsType;

    @SuppressWarnings("unchecked")
    public LocalFileSystem() {
        settingsType = (Class<T>) FileSystemSettings.class;
    }

    public LocalFileSystem(@NonNull Class<T> settingsType) {
        this.settingsType = settingsType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public FileSystem<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                   String pathPrefix,
                                   @NonNull BaseEnv<?> env) throws IOException {
        try {
            if (Strings.isNullOrEmpty(pathPrefix)) {
                pathPrefix = __CONFIG_PATH;
            }
            super.init(config, pathPrefix, env, settingsType);
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
        return new LocalPathInfo(values);
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Inode get(@NonNull String path, String domain) throws IOException {
        if (!path.startsWith(settings().pathPrefix())) {
            path = PathUtils.formatPath(String.format("%s/%s/%s", settings.pathPrefix(), domain, path));
        }
        PathInfo pi = new LocalPathInfo(path, domain);
        return getInode(pi);
    }

    @Override
    protected String getAbsolutePath(@NonNull String path,
                                     @NonNull String module) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(module));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        String target = domainMap.get(module);

        return PathUtils.formatPath(String.format("/%s/%s/%s",
                settings.pathPrefix(), target, path));
    }

    @Override
    public DirectoryInode mkdir(@NonNull DirectoryInode parent,
                                @NonNull String name) throws IOException {
        String path = PathUtils.formatPath(String.format("%s/%s", parent.getAbsolutePath(), name));
        LocalPathInfo pd = (LocalPathInfo) parent.getPathInfo();
        if (pd == null) {
            pd = new LocalPathInfo(parent.getPath());
            parent.setPathInfo(pd);
        }
        if (!pd.file().exists()) {
            throw new IOException(String.format("Parent directory not found. [path=%s]", pd.file().getAbsolutePath()));
        }
        LocalPathInfo pi = new LocalPathInfo(path, pd.domain());
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
        LocalPathInfo pi = new LocalPathInfo(path, module);
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
        LocalPathInfo pi = new LocalPathInfo(path, module);
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

    @Override
    public FileInode upload(@NonNull File source,
                            @NonNull DirectoryInode directory) throws IOException {
        LocalPathInfo di = (LocalPathInfo) directory.getPathInfo();
        if (di == null) {
            di = new LocalPathInfo(directory.getPath());
            directory.setPathInfo(di);
        }
        if (!di.file().exists()) {
            throw new IOException(String.format("Target directory not found. [path=%s]", di.file().getAbsolutePath()));
        }
        String fname = FilenameUtils.getName(source.getAbsolutePath());
        String path = PathUtils.formatPath(String.format("%s/%s", di.file().getAbsolutePath(), fname));
        File dest = new File(path);
        if (dest.getAbsolutePath().compareTo(source.getAbsolutePath()) != 0) {
            FileUtils.copyFile(source, dest);
        }
        return create(di.domain(), dest.getAbsolutePath());
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
    protected Reader getReader(@NonNull Inode inode) throws IOException {
        return null;
    }

    @Override
    protected Writer getWriter(@NonNull Inode inode, boolean createDir, boolean overwrite) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) inode.getPathInfo();
        if (pi == null) {
            pi = new LocalPathInfo(inode.getPath());
            inode.setPathInfo(pi);
        }

        return null;
    }

    /**
     * @param path
     * @param createDir
     * @param overwrite
     * @return
     * @throws IOException
     */
    @Override
    public Writer writer(@NonNull PathInfo path, boolean createDir, boolean overwrite) throws IOException {
        if (!(path instanceof LocalPathInfo)) {
            throw new IOException(String.format("Invalid PathInfo instance. [passed=%s]",
                    path.getClass().getCanonicalName()));
        }
        if (createDir) {
            File dir = ((LocalPathInfo) path).file().getParentFile();
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    throw new IOException(String.format("Failed to create parent folder. [path=%s]", dir.getAbsolutePath()));
                }
            }
        }
        return new LocalWriter(path).open(overwrite);
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Reader reader(@NonNull PathInfo path) throws IOException {
        if (!(path instanceof LocalPathInfo)) {
            throw new IOException(String.format("Invalid PathInfo instance. [passed=%s]",
                    path.getClass().getCanonicalName()));
        }
        if (!path.exists()) {
            throw new IOException(String.format("File not found. [path=%s]", ((LocalPathInfo) path).file().getAbsolutePath()));
        }
        return new LocalReader(path).open();
    }
}
