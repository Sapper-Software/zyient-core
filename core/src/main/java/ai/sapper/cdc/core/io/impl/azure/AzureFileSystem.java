package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.FileUploadCallback;
import ai.sapper.cdc.core.io.impl.RemoteFileSystem;
import ai.sapper.cdc.core.io.model.*;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlockBlobItem;
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
            AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
            KeyStore keyStore = env().keyStore();
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
            Preconditions.checkNotNull(((AzureFileSystemSettings) settings).getClientSettings());
            KeyStore keyStore = env().keyStore();
            Preconditions.checkNotNull(keyStore);
            AzureFileSystemSettings afs = (AzureFileSystemSettings) this.settings;
            client = new AzureFsClient()
                    .init(afs.getClientSettings(), keyStore);
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
    public FileInode create(@NonNull DirectoryInode dir, @NonNull String name) throws IOException {
        FileInode node = (FileInode) createInode(dir, name, InodeType.File);
        if (node.getPathInfo() == null) {
            PathInfo pi = parsePathInfo(node.getPath());
            node.setPathInfo(pi);
        }
        AzurePathInfo pi = (AzurePathInfo) node.getPathInfo();
        Preconditions.checkNotNull(pi);
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (FileInode) updateInodeWithLock(node);
    }

    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof AzurePathInfo);
        boolean ret = false;
        if (deleteInode(path, recursive)) {
            AzurePathInfo pi = (AzurePathInfo) path;
            BlobContainerClient cc = client.getContainer(pi.container());
            if (cc != null && cc.exists()) {
                AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
                if (!settings.isUseHierarchical()) {
                    if (!pi.directory()) {
                        String name = getBlobName(pi);
                        BlobClient bc = cc.getBlobClient(name);
                        if (bc.exists()) {
                            DefaultLogger.trace(String.format("Deleting Azure BLOB: [name=%s]", bc.getBlobName()));
                            bc.delete();
                            return true;
                        }
                    } else {
                        return true;
                    }
                } else {
                    ListBlobsOptions options = new ListBlobsOptions()
                            .setPrefix(path.path());
                    Iterable<BlobItem> blobs = cc.listBlobsByHierarchy(DELIMITER, options, null);
                    if (blobs != null) {
                        while (blobs.iterator().hasNext()) {
                            BlobItem bi = blobs.iterator().next();
                            if (bi.isDeleted()) continue;
                            if (!recursive) {
                                if (bi.getName().compareTo(pi.path()) != 0) {
                                    continue;
                                }
                            }
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
        }
        return ret;
    }

    @Override
    protected PathInfo parsePathInfo(@NonNull DirectoryInode parent,
                                     @NonNull String path,
                                     @NonNull InodeType type) throws IOException {
        String p = PathUtils.formatPath(String.format("%s/%s", parent.getAbsolutePath(), path));
        AzurePathInfo pi = null;
        if (parent.getPathInfo() == null) {
            pi = (AzurePathInfo) parsePathInfo(parent.getPath());
            parent.setPathInfo(pi);
        } else {
            pi = (AzurePathInfo) parent.getPathInfo();
        }
        return new AzurePathInfo(this, pi.domain(), pi.container(), p, type);
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        if (path instanceof AzurePathInfo) {
            if (path.directory()) {
                return true;
            }
            AzurePathInfo pi = (AzurePathInfo) path;
            BlobContainerClient cc = client.getContainer(pi.container());
            if (cc != null && cc.exists()) {
                AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
                String name = pi.path();
                if (!settings.isUseHierarchical()) {
                    name = getBlobName(pi);
                }
                return cc.getBlobClient(name).exists();
            }
        }
        return false;
    }

    private static String getBlobName(AzurePathInfo pathInfo) {
        return String.format("%s.blob", pathInfo.uuid());
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        AzurePathInfo pi = (AzurePathInfo) inode.getPathInfo();
        if (pi == null) {
            AzureContainer container = (AzureContainer) domainMap.get(inode.getDomain());
            pi = new AzurePathInfo(this, inode, container.getContainer());
            inode.setPathInfo(pi);
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

    @SuppressWarnings("unchecked")
    @Override
    public void onSuccess(@NonNull FileInode inode,
                          @NonNull Object response,
                          boolean clearLock) {
        Preconditions.checkArgument(response instanceof Response);
        try {
            Response<BlockBlobItem> r = (Response<BlockBlobItem>) response;
            inode.setSyncTimestamp(r.getValue().getLastModified().toInstant().toEpochMilli());
            long size = size(inode.getPathInfo());
            inode.setSyncedSize(size);
            if (clearLock) {
                inode.getState().setState(EFileState.Synced);
                fileUnlock(inode);
            } else {
                inode.getState().setState(EFileState.Updating);
                fileUpdateLock(inode);
            }
            updateInodeWithLock(inode);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(LOG, ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }

    @Override
    public PathInfo createSubPath(@NonNull PathInfo parent, @NonNull String path) {
        Preconditions.checkArgument(parent instanceof AzurePathInfo);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        return new AzurePathInfo(this,
                parent.domain(),
                ((AzurePathInfo) parent).container(),
                path,
                InodeType.Directory);
    }

    @Override
    public PathInfo createPath(@NonNull String domain,
                               @NonNull Container container,
                               @NonNull String path,
                               @NonNull InodeType type) {
        Preconditions.checkArgument(container instanceof AzureContainer);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        return new AzurePathInfo(this, domain, ((AzureContainer) container).getContainer(), path, type);
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
        AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
        AzureFileUploader task = new AzureFileUploader(this, client, inode, source,
                this, clearLock,
                settings.getUploadTimeout(), settings.isUseHierarchical());
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
                AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
                String name = path.path();
                if (!settings.isUseHierarchical()) {
                    name = getBlobName(path);
                }
                BlobClient bc = cc.getBlobClient(name);
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
        private final long uploadTimeout;
        private final boolean useHierarchical;
        private final File source;

        protected AzureFileUploader(@NonNull RemoteFileSystem fs,
                                    @NonNull AzureFsClient client,
                                    @NonNull FileInode inode,
                                    @NonNull File source,
                                    @NonNull FileUploadCallback callback,
                                    boolean clearLock,
                                    long uploadTimeout,
                                    boolean useHierarchical) {
            super(fs, inode, callback, clearLock);
            this.client = client;
            this.source = source;
            this.uploadTimeout = uploadTimeout;
            this.useHierarchical = useHierarchical;
        }

        @Override
        protected Object upload() throws Throwable {
            AzurePathInfo pi = (AzurePathInfo) inode.getPathInfo();
            if (pi == null) {
                throw new Exception("Azure Path information not specified...");
            }
            if (!source.exists()) {
                throw new IOException(String.format("Source file not found. [path=%s]", source.getAbsolutePath()));
            }
            try {
                BlobContainerClient cc = client.getContainer(pi.container());
                if (cc == null || !cc.exists()) {
                    throw new IOException(String.format("Azure Container not found. [container=%s]", pi.container()));
                }
                BlobUploadFromFileOptions options = new BlobUploadFromFileOptions(source.getAbsolutePath());
                Duration timeout = Duration.ofSeconds(uploadTimeout);
                String name = pi.path();
                if (!useHierarchical) {
                    name = getBlobName(pi);
                }
                BlobClient bc = cc.getBlobClient(name);
                return bc.uploadFromFileWithResponse(options, timeout, null);
            } catch (RuntimeException re) {
                DefaultLogger.stacktrace(re);
                throw new Exception(re);
            }
        }
    }

    public static class AzureFileSystemConfigReader extends RemoteFileSystemConfigReader {

        public AzureFileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, AzureFileSystemSettings.class, AzureContainer.class);
        }
    }

}
