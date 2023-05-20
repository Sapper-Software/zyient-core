package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.FileUploadCallback;
import ai.sapper.cdc.core.io.impl.RemoteFileSystem;
import ai.sapper.cdc.core.io.model.Container;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.FileSystemSettings;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class AzureFileSystem extends RemoteFileSystem {
    public static final String DELIMITER = "/";

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
    public Class<? extends FileSystemSettings> getSettingsType() {
        return AzureFileSystemSettings.class;
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
            settings.setClientSettings(client.settings());
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
            Preconditions.checkNotNull(((AzureFileSystemSettings) settings).getClientSettings());
            client = new AzureFsClient()
                    .init(((AzureFileSystemSettings) settings).getClientSettings(), keyStore);
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
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Preconditions.checkArgument(pathInfo instanceof AzurePathInfo);
        return super.create(pathInfo);
    }

    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof AzurePathInfo);
        boolean ret = false;
        if (deleteInode(path, recursive)) {
            AzurePathInfo pi = (AzurePathInfo) path;
            BlobContainerClient cc = client.getContainer(pi.container());
            if (cc != null && cc.exists()) {
                ListBlobsOptions options = new ListBlobsOptions()
                        .setPrefix(path.path());
                Iterable<BlobItem> blobs = cc.listBlobsByHierarchy(DELIMITER, options, null);
                if (blobs != null) {
                    while (blobs.iterator().hasNext()) {
                        BlobItem bi = blobs.iterator().next();
                        if (bi.isDeleted()) continue;
                        BlobClient bc = cc.getBlobClient(bi.getName());
                        if (bc != null) {
                            DefaultLogger.trace(String.format("Deleting Azure BLOB: [name=%s]", bi.getName()));
                            bc.delete();
                            ret = true;
                        }
                    }
                }
            }
        }
        return ret;
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        if (path instanceof AzurePathInfo) {
            if (path.directory()) {
                return true;
            }
            AzurePathInfo pi = (AzurePathInfo) path;
            BlobContainerClient cc = client.getContainer(pi.container());
            if (cc != null && cc.exists())
                return cc.getBlobClient(pi.path()).exists();
        }
        return false;
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        AzurePathInfo pi = (AzurePathInfo) inode.getPathInfo();
        if (pi == null) {
            AzureContainer container = (AzureContainer) domainMap.get(inode.getDomain());
            pi = new AzurePathInfo(this, inode, container.getContainer());
            inode.setPathInfo(pi);
        }
        if (!pi.exists()) {
            throw new IOException(String.format("Local file not found. [path=%s]", inode.getAbsolutePath()));
        }
        return new AzureReader(this, inode).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, boolean overwrite) throws IOException {
        AzurePathInfo pi = (AzurePathInfo) inode.getPathInfo();
        if (pi == null) {
            AzureContainer container = (AzureContainer) domainMap.get(inode.getDomain());
            pi = new AzurePathInfo(this, inode, container.getContainer());
            inode.setPathInfo(pi);
        }
        return new AzureWriter(inode, this, overwrite).open();
    }

    @Override
    public void onSuccess(@NonNull FileInode inode,
                          @NonNull Object response,
                          boolean clearLock) {

    }

    @Override
    public PathInfo createSubPath(@NonNull PathInfo parent, @NonNull String path) {
        Preconditions.checkArgument(parent instanceof AzurePathInfo);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        return new AzurePathInfo(this, parent.domain(), ((AzurePathInfo) parent).container(), path);
    }

    @Override
    public PathInfo createPath(@NonNull String domain, @NonNull Container container, @NonNull String path) {
        Preconditions.checkArgument(container instanceof AzureContainer);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        return new AzurePathInfo(this, domain, ((AzureContainer) container).getContainer(), path);
    }

    @Override
    public FileInode upload(@NonNull File source,
                            @NonNull FileInode inode,
                            boolean clearLock) throws IOException {
        AzurePathInfo path = (AzurePathInfo) inode.getPathInfo();
        if (path == null) {
            throw new IOException(
                    String.format("Path information not set in inode. [domain=%s, path=%s]",
                            inode.getDomain(), inode.getAbsolutePath()));
        }
        AzureFileUploader task = new AzureFileUploader(this, client, inode, this, clearLock);
        uploader.submit(task);
        return inode;
    }

    @Override
    public File download(@NonNull FileInode inode) throws IOException {
        AzurePathInfo path = (AzurePathInfo) inode.getPathInfo();
        if (path == null) {
            throw new IOException(
                    String.format("Path information not set in inode. [domain=%s, path=%s]",
                            inode.getDomain(), inode.getAbsolutePath()));
        }
        if (exists(path)) {
            if (path.temp() == null) {
                File tmp = getInodeTempPath(path);
                if (tmp.exists()) {
                    if (!tmp.delete()) {
                        throw new IOException(
                                String.format("Error deleting temporary file. [path=%s]", tmp.getAbsolutePath()));
                    }
                }
                path.withTemp(tmp);
            }
            BlobContainerClient cc = client.getContainer(path.container());
            if (cc != null && cc.exists()) {
                BlobClient bc = cc.getBlobClient(path.path());
                if (bc != null && bc.exists()) {
                    try (FileOutputStream fos = new FileOutputStream(path.temp())) {
                        bc.downloadStream(fos);
                    }
                    return path.temp();
                }
            }
        }
        return null;
    }

    @Override
    public long size(@NonNull PathInfo path) throws IOException {
        if (path instanceof AzurePathInfo) {
            AzurePathInfo ap = (AzurePathInfo) path;
            BlobClient c = client.getContainer(ap.container()).getBlobClient(ap.path());
            if (c != null && c.exists())
                return c.getProperties().getBlobSize();
        }
        throw new IOException(String.format("Invalid Path handle. [type=%s]", path.getClass().getCanonicalName()));
    }

    public static class AzureFileUploader extends FileUploader {
        private final AzureFsClient client;

        protected AzureFileUploader(@NonNull RemoteFileSystem fs,
                                    @NonNull AzureFsClient client,
                                    @NonNull FileInode inode,
                                    @NonNull FileUploadCallback callback,
                                    boolean clearLock) {
            super(fs, inode, callback, clearLock);
            this.client = client;
        }

        @Override
        protected Object upload() throws Throwable {
            AzurePathInfo pi = (AzurePathInfo) inode.getPathInfo();
            if (pi == null) {
                throw new Exception("Azure Path information not specified...");
            }
            File source = pi.temp();
            if (source == null) {
                throw new Exception("File to upload not specified...");
            }
            if (!source.exists()) {
                throw new IOException(String.format("Source file not found. [path=%s]", source.getAbsolutePath()));
            }
            BlobContainerClient cc = client.getContainer(pi.container());
            if (cc == null || !cc.exists()) {
                throw new IOException(String.format("Azure Container not found. [container=%s]", pi.container()));
            }
            BlobUploadFromFileOptions options = new BlobUploadFromFileOptions(source.getAbsolutePath());
            options.setTags(pi.pathConfig());
            Duration timeout = Duration.ofSeconds(10);
            BlobClient bc = cc.getBlobClient(pi.path());
            return bc.uploadFromFileWithResponse(options, timeout, null);
        }
    }

    public static class AzureFileSystemConfigReader extends RemoteFileSystemConfigReader {

        public AzureFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, AzureFileSystemSettings.class, AzureContainer.class);
        }
    }

}
