package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.io.model.*;
import ai.sapper.cdc.core.keystore.KeyStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class FileSystem implements Closeable {
    private PathInfo root;
    private FileSystemHelper helper = null;
    private ZookeeperConnection zkConnection;
    protected ConfigReader configReader;

    public abstract FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    String pathPrefix,
                                    KeyStore keyStore,
                                    @NonNull ConnectionManager manager) throws IOException;

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                     String pathPrefix,
                     KeyStore keyStore,
                     @NonNull ConnectionManager manager,
                     @NonNull Class<? extends FileSystemSettings> settingsType) throws Exception {

    }

    public abstract Inode create(@NonNull InodeType type,
                                 @NonNull String path,
                                 String domain,
                                 boolean overwrite) throws IOException;

    public abstract Inode get(@NonNull String path, String domain) throws IOException;

    public abstract Inode get(@NonNull String path, String domain, boolean prefix) throws IOException;

    public abstract Inode get(@NonNull Map<String, String> config);

    public abstract DirectoryInode mkdir(@NonNull DirectoryInode path, @NonNull String name) throws IOException;

    public abstract DirectoryInode mkdirs(@NonNull PathInfo path) throws IOException;

    public abstract FileInode upload(@NonNull File source, @NonNull DirectoryInode directory) throws IOException;

    public abstract boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException;


    public boolean delete(@NonNull PathInfo path) throws IOException {
        return delete(path, false);
    }

    public abstract List<String> list(@NonNull PathInfo path, boolean recursive) throws IOException;

    public abstract List<String> find(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException;

    public abstract List<String> findFiles(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException;

    public abstract String tempPath();

    public FileSystem setRootPath(@NonNull PathInfo rootPath) {
        this.root = rootPath;
        return this;
    }

    public boolean exists(@NonNull String path, String domain) throws IOException {
        Inode pi = get(path, domain);
        if (pi != null) return pi.exists();
        return false;
    }

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

    public abstract Writer writer(@NonNull PathInfo path, boolean createDir, boolean overwrite) throws IOException;

    public Writer writer(@NonNull PathInfo path, boolean overwrite) throws IOException {
        return writer(path, false, overwrite);
    }

    public Writer writer(@NonNull PathInfo path) throws IOException {
        return writer(path, false, false);
    }

    public abstract Reader reader(@NonNull PathInfo path) throws IOException;

    @Getter
    @Setter
    public static class FileSystemSettings extends Settings {
        public static final String TEMP_PATH = String.format("%s/zyient/cdc",
                System.getProperty("java.io.tmpdir"));

        public static final String CONFIG_ROOT = "root";
        public static final String CONFIG_TEMP_FOLDER = "tempDir";
        public static final String CONFIG_ZK_CONNECTION = "zk";

        @Config(name = CONFIG_ZK_CONNECTION)
        private String zkConnection;
        @Config(name = CONFIG_ROOT)
        private String rootPath;
        @Config(name = CONFIG_TEMP_FOLDER, required = false)
        private String tempDir = TEMP_PATH;
        @Config(name = "domains", required = false)
        private Map<String, String> domains;
    }

    public interface FileSystemMocker {
        FileSystem create(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws Exception;
    }
}
