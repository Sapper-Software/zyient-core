/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.filesystem.impl.azure;

import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobUploadFromUrlOptions;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.azure.AzureFsClient;
import io.zyient.base.core.connections.azure.AzureFsHelper;
import io.zyient.base.core.keystore.KeyStore;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.Writer;
import io.zyient.core.filesystem.impl.FileUploadCallback;
import io.zyient.core.filesystem.impl.PostOperationVisitor;
import io.zyient.core.filesystem.impl.RemoteFileSystem;
import io.zyient.core.filesystem.model.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class AzureFileSystem extends RemoteFileSystem {

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
            client = (AzureFsClient) new AzureFsClient()
                    .init(configReader.config(), env);
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
            client = (AzureFsClient) new AzureFsClient()
                    .setup(afs.getClientSettings(), env);
            return postInit();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(LOG, "Error initializing Local FileSystem.", t);
            state().error(t);
            throw new IOException(t);
        }
    }

    @Override
    protected boolean createDomainDir(DirectoryInode dir) throws IOException {
        return true;
    }

    @Override
    public PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException {
        return new AzurePathInfo(this, values);
    }

    @Override
    public PathInfo parsePathInfo(@NonNull String domain, @NonNull String path) throws IOException {
        AzureContainer container = (AzureContainer) domainMap.get(domain);
        return new AzurePathInfo(this, domain, container.getContainer(), path, InodeType.Directory);
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Preconditions.checkArgument(pathInfo instanceof AzurePathInfo);
        return super.create(pathInfo);
    }

    @Override
    public FileInode __create(@NonNull DirectoryInode dir, @NonNull String name) throws IOException {
        FileInode node = (FileInode) createInode(dir, name, InodeType.File);
        AzurePathInfo pi = checkAndGetPath(node);
        Preconditions.checkNotNull(pi);
        if (node.getURI() == null)
            node.setURI(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (FileInode) updateInodeWithLock(node);
    }

    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof AzurePathInfo);
        AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
        if (deleteInode(path, recursive)) {
            AzurePathInfo pi = (AzurePathInfo) path;
            if (pi.directory()) {
                if (!settings.isUseHierarchical()) return true;
                else {
                    return AzureFsHelper.delete(client.client(),
                            pi.container(),
                            true,
                            pi.fsPath(),
                            recursive);
                }
            }
            String name = getBlobName(pi);
            return AzureFsHelper.delete(client.client(),
                    pi.container(),
                    settings.isUseHierarchical(),
                    name,
                    recursive);
        }
        return false;
    }

    @Override
    protected PathInfo __parsePathInfo(@NonNull DirectoryInode parent,
                                       @NonNull String path,
                                       @NonNull InodeType type) throws IOException {
        String p = PathUtils.formatPath(String.format("%s/%s", parent.getPath(), path));
        AzurePathInfo pi = checkAndGetPath(parent);
        return new AzurePathInfo(this, pi.domain(), pi.container(), p, type);
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        if (path instanceof AzurePathInfo pi) {
            AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
            Inode node = getInode(path);
            if (node == null) {
                return false;
            }
            if (path.directory()) {
                if (settings.isUseHierarchical())
                    return true;
                else {
                    return AzureFsHelper.exists(client.client(),
                            pi.container(),
                            pi.fsPath());
                }
            }
            return AzureFsHelper.exists(client.client(),
                    pi.container(),
                    getBlobName(pi));
        }
        return false;
    }

    private String getBlobName(AzurePathInfo pathInfo) {
        AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
        if (settings.isUseHierarchical()) {
            return pathInfo.fsPath();
        }
        return String.format("%s.blob", pathInfo.uuid());
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        AzurePathInfo pi = checkAndGetPath(inode);
        return new AzureReader(this, inode).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, boolean overwrite) throws IOException {
        AzurePathInfo pi = checkAndGetPath(inode);
        return new AzureWriter(inode, this, overwrite).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, @NonNull File temp) throws IOException {
        AzurePathInfo pi = checkAndGetPath(inode);
        return new AzureWriter(inode, this, temp).open();
    }

    @Override
    protected void doCopy(@NonNull FileInode source, @NonNull FileInode target) throws IOException {
        AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
        AzurePathInfo sp = checkAndGetPath(source);
        AzurePathInfo tp = checkAndGetPath(target);
        BlobContainerClient sc = client.getContainer(sp.container());
        if (sc == null || !sc.exists()) {
            throw new IOException(String.format("Azure Container not found. [container=%s]", sp.container()));
        }
        BlobContainerClient tc = client.getContainer(tp.container());
        if (tc == null || !tc.exists()) {
            throw new IOException(String.format("Azure Container not found. [container=%s]", tp.container()));
        }
        try {
            BlobClient bs = sc.getBlobClient(getBlobName(sp));
            BlobClient bt = tc.getBlobClient(getBlobName(tp));
            BlobServiceSasSignatureValues sas = new BlobServiceSasSignatureValues(OffsetDateTime.now().plusHours(1),
                    BlobContainerSasPermission.parse("r"));
            String sasToken = bs.generateSas(sas);
            BlobUploadFromUrlOptions options = new BlobUploadFromUrlOptions(bs.getBlobUrl() + "?" + sasToken)
                    .setCopySourceBlobProperties(true);
            Duration timeout = Duration.ofSeconds(settings.getUploadTimeout().normalized());
            Response<BlockBlobItem> response = bt.getBlockBlobClient()
                    .uploadFromUrlWithResponse(options, timeout, null);
            target.setSyncTimestamp(response.getValue().getLastModified().toInstant().toEpochMilli());
        } catch (Throwable re) {
            throw new IOException(re);
        }
    }

    @Override
    protected PathInfo renameFile(@NonNull FileInode source,
                                  @NonNull String name) throws IOException {
        AzurePathInfo pi = checkAndGetPath(source);
        String path = String.format("%s/%s", pi.parent(), name);
        return new AzurePathInfo(this, pi.domain(), pi.container(), path, InodeType.File);
    }

    @Override
    protected void doMove(@NonNull FileInode source, @NonNull FileInode target) throws IOException {
        doCopy(source, target);
        AzurePathInfo sp = checkAndGetPath(source);
        delete(sp, false);
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
            for (PostOperationVisitor visitor : visitors()) {
                visitor.visit(PostOperationVisitor.Operation.Upload,
                        PostOperationVisitor.OperationState.Completed,
                        inode, null);
            }
        } catch (Exception ex) {
            for (PostOperationVisitor visitor : visitors()) {
                visitor.visit(PostOperationVisitor.Operation.Upload,
                        PostOperationVisitor.OperationState.Error,
                        inode, ex);
            }
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
        try {
            AzurePathInfo path = checkAndGetPath(inode);
            AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
            AzureFileUploader task = new AzureFileUploader(this, client, inode, source,
                    this, clearLock,
                    settings.getUploadTimeout().normalized());
            uploader.submit(task);
            return inode;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public File download(@NonNull FileInode inode, long timeout) throws IOException {
        AzureFileSystemSettings settings = (AzureFileSystemSettings) this.settings;
        AzurePathInfo path = checkAndGetPath(inode);
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
            if (!checkInodeAvailable(inode, timeout)) {
                throw new IOException(
                        String.format("Download operation timeout: File not available for download. [path=%s]",
                                inode.getURI()));
            }
            return AzureFsHelper.download(client.client(),
                    path.container(),
                    getBlobName(path),
                    path.temp().getAbsolutePath(),
                    settings.getRetryCount());

        }
        return null;
    }

    @Override
    public long size(@NonNull PathInfo path) throws IOException {
        if (path instanceof AzurePathInfo ap) {
            String name = getBlobName(ap);
            BlobClient c = client.getContainer(ap.container()).getBlobClient(name);
            if (c.exists())
                return c.getProperties().getBlobSize();
            else {
                throw new IOException(String.format("File not found. [path=%s]", path.pathConfig()));
            }
        }
        throw new IOException(String.format("Invalid Path handle. [type=%s]", path.getClass().getCanonicalName()));
    }

    public static class AzureFileUploader extends FileUploader {
        private final AzureFsClient client;
        private final long uploadTimeout;
        private final File source;

        protected AzureFileUploader(@NonNull RemoteFileSystem fs,
                                    @NonNull AzureFsClient client,
                                    @NonNull FileInode inode,
                                    @NonNull File source,
                                    @NonNull FileUploadCallback callback,
                                    boolean clearLock,
                                    long uploadTimeout) {
            super(fs, inode, callback, clearLock);
            this.client = client;
            this.source = source;
            this.uploadTimeout = uploadTimeout;
        }

        @Override
        protected Object upload() throws Throwable {
            Preconditions.checkArgument(fs instanceof AzureFileSystem);
            AzurePathInfo pi = fs.checkAndGetPath(inode);
            if (!source.exists()) {
                throw new IOException(String.format("Source file not found. [path=%s]", source.getAbsolutePath()));
            }
            try {
                String name = ((AzureFileSystem) fs).getBlobName(pi);
                return AzureFsHelper.upload(client.client(),
                        pi.container(),
                        name,
                        uploadTimeout,
                        source);
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
