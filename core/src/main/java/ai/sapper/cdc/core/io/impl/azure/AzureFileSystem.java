package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.RemoteFileSystem;
import ai.sapper.cdc.core.io.model.DirectoryInode;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.storage.blob.BlobClient;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class AzureFileSystem extends RemoteFileSystem {
    private AzureFsClient client;
    private AzureFileSystemSettings settings;

    public static AzurePathInfo checkPath(PathInfo pathInfo) throws IOException {
        if (!(pathInfo instanceof AzurePathInfo)) {
            throw new IOException(
                    String.format("Invalid Path type. [type=%s]", pathInfo.getClass().getCanonicalName()));
        }
        return (AzurePathInfo) pathInfo;
    }

    @Override
    public FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                            @NonNull BaseEnv<?> env) throws IOException {
        try {
            AzureFileSystemConfigReader configReader = new AzureFileSystemConfigReader(config);
            super.init(config, env, configReader);
            settings = (AzureFileSystemSettings) configReader.settings();
            KeyStore keyStore = env.keyStore();
            Preconditions.checkNotNull(keyStore);
            client = new AzureFsClient()
                    .init(configReader.config(), keyStore);
            settings.clientSettings = client.settings();
            return postInit();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(LOG, "Error initializing Local FileSystem.", t);
            state().error(t);
            throw new IOException(t);
        }
    }

    @Override
    public FileSystem init(@NonNull FileSystemSettings settings,
                            @NonNull BaseEnv<?> env) throws IOException {
        Preconditions.checkArgument(settings instanceof AzureFileSystemSettings);
        super.init(settings, env);
        try {
            this.settings = (AzureFileSystemSettings) settings;
            KeyStore keyStore = env.keyStore();
            Preconditions.checkNotNull(keyStore);
            Preconditions.checkNotNull(((AzureFileSystemSettings) settings).clientSettings);
            client = new AzureFsClient()
                    .init(((AzureFileSystemSettings) settings).clientSettings, keyStore);
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
        return new AzurePathInfo(this, values);
    }

    @Override
    public DirectoryInode mkdir(@NonNull DirectoryInode path, @NonNull String name) throws IOException {
        return null;
    }

    @Override
    public DirectoryInode mkdirs(@NonNull String domain, @NonNull String path) throws IOException {
        return null;
    }

    @Override
    public FileInode create(@NonNull String domain, @NonNull String path) throws IOException {
        return null;
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        return null;
    }

    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        return false;
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        return false;
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        return null;
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, boolean overwrite) throws IOException {
        return null;
    }

    @Override
    public void onSuccess(@NonNull FileInode inode, @NonNull Object response, boolean clearLock) {

    }

    @Override
    public void onError(@NonNull FileInode inode, @NonNull Throwable error) {

    }

    @Override
    public FileInode upload(@NonNull File source, @NonNull FileInode path, boolean clearLock) throws IOException {
        return null;
    }

    @Override
    public File download(@NonNull FileInode inode) throws IOException {
        return null;
    }

    @Override
    public long size(@NonNull PathInfo path) throws IOException {
        if (path instanceof AzurePathInfo) {
            AzurePathInfo ap = (AzurePathInfo) path;
            BlobClient c = client.getContainer(ap.container()).getBlobClient(ap.path());
            return c.getProperties().getBlobSize();
        }
        throw new IOException(String.format("Invalid Path handle. [type=%s]", path.getClass().getCanonicalName()));
    }

    public static class AzureFileSystemConfigReader extends RemoteFileSystemConfigReader {
        public static final String __CONFIG_PATH = "azure/fs";

        public AzureFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, AzureFileSystemSettings.class, AzureContainer.class);
        }
    }

    @Getter
    @Setter
    public static class AzureFileSystemSettings extends RemoteFileSystemSettings {
        private AzureFsClient.AzureFsClientSettings clientSettings;
    }
}
