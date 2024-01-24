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

package io.zyient.core.filesystem.impl.s3;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.aws.auth.S3StorageAuth;
import io.zyient.base.core.connections.aws.auth.S3StorageAuthSettings;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.Writer;
import io.zyient.core.filesystem.impl.FileUploadCallback;
import io.zyient.core.filesystem.impl.PostOperationVisitor;
import io.zyient.core.filesystem.impl.RemoteFileSystem;
import io.zyient.core.filesystem.model.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class S3FileSystem extends RemoteFileSystem {

    @Getter(AccessLevel.PACKAGE)
    private S3Client client;

    public S3FileSystem withClient(@NonNull S3Client client) {
        this.client = client;
        return this;
    }

    private boolean bucketExists(String bucket) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucket)
                .build();
        try {
            client.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    @Override
    public Class<? extends FileSystemSettings> getSettingsType() {
        return S3FileSystemSettings.class;
    }

    @Override
    public FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws IOException {
        try {
            super.init(config, env, new S3FileSystemConfigReader(config));
            S3FileSystemSettings settings = (S3FileSystemSettings) configReader().settings();
            s3Init();
            return postInit();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(LOG, "Error initializing Local FileSystem.", t);
            state().error(t);
            throw new IOException(t);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public FileSystem init(@NonNull FileSystemSettings settings, @NonNull BaseEnv<?> env) throws IOException {
        Preconditions.checkArgument(settings instanceof S3FileSystemSettings);
        super.init(settings, env);
        try {
            s3Init();
            return postInit();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(LOG, "Error initializing Local FileSystem.", t);
            state().error(t);
            throw new IOException(t);
        }
    }

    @SuppressWarnings("unchecked")
    private void s3Init() throws Exception {
        S3FileSystemSettings s3settings = (S3FileSystemSettings) this.settings;
        if (client == null) {
            S3StorageAuth auth = null;
            if (ConfigReader.checkIfNodeExists(configReader().config(), S3StorageAuthSettings.__CONFIG_PATH)) {
                HierarchicalConfiguration<ImmutableNode> ac =
                        configReader().config().configurationAt(S3StorageAuthSettings.__CONFIG_PATH);
                Class<? extends S3StorageAuth> clazz = (Class<? extends S3StorageAuth>) ConfigReader.readType(ac);
                auth = clazz.getDeclaredConstructor()
                        .newInstance();
                auth.init(configReader().config(), env().keyStore());
            }
            Region region = Region.of(s3settings.getRegion());
            S3ClientBuilder builder = S3Client.builder()
                    .region(region);
            if (!Strings.isNullOrEmpty(((S3FileSystemSettings) settings).getEndpoint())) {
                builder.endpointOverride(new URI(((S3FileSystemSettings) settings).getEndpoint()));
            }
            if (auth != null) {
                builder.credentialsProvider(auth.credentials());
            }
            client = builder.build();
        }
    }

    @Override
    protected boolean createDomainDir(DirectoryInode dir) throws IOException {
        return true;
    }

    @Override
    public PathInfo parsePathInfo(@NonNull Map<String, String> values) throws IOException {
        return new S3PathInfo(this, values);
    }

    @Override
    public PathInfo parsePathInfo(@NonNull String domain, @NonNull String path) throws IOException {
        S3Container container = (S3Container) domainMap.get(domain);
        return new S3PathInfo(this, domain, container.getBucket(), path, InodeType.Directory);
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Preconditions.checkArgument(pathInfo instanceof S3PathInfo);
        return super.create(pathInfo);
    }

    @Override
    public FileInode create(@NonNull DirectoryInode dir,
                            @NonNull String name) throws IOException {
        FileInode node = (FileInode) createInode(dir, name, InodeType.File);
        S3PathInfo pi = checkAndGetPath(node);
        Preconditions.checkNotNull(pi);
        if (node.getURI() == null)
            node.setURI(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (FileInode) updateInodeWithLock(node);
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof S3PathInfo);
        if (deleteInode(path, recursive)) {
            S3PathInfo s3path = (S3PathInfo) path;
            if (bucketExists(s3path.bucket())) {
                if (recursive) {
                    boolean ret = true;
                    ListObjectsRequest request = ListObjectsRequest
                            .builder()
                            .bucket(s3path.bucket())
                            .prefix(path.path())
                            .build();

                    ListObjectsResponse res = client.listObjects(request);
                    List<S3Object> objects = res.contents();
                    for (S3Object obj : objects) {
                        DeleteObjectRequest dr = DeleteObjectRequest.builder()
                                .bucket(s3path.bucket())
                                .key(obj.key())
                                .build();
                        DeleteObjectResponse r = client.deleteObject(dr);
                        SdkHttpResponse sr = r.sdkHttpResponse();
                        if (sr.statusCode() < 200 || sr.statusCode() >= 300) {
                            String mesg = JSONUtils.asString(sr);
                            throw new IOException(mesg);
                        }
                    }
                    return ret;
                } else if (!path.directory()) {
                    DeleteObjectRequest dr = DeleteObjectRequest.builder()
                            .bucket(s3path.bucket())
                            .key(s3path.fsPath())
                            .build();
                    DeleteObjectResponse r = client.deleteObject(dr);
                    SdkHttpResponse sr = r.sdkHttpResponse();
                    if (sr.statusCode() >= 200 && sr.statusCode() < 300) {
                        return true;
                    } else {
                        String mesg = JSONUtils.asString(sr);
                        throw new IOException(mesg);
                    }
                }
            }
        }
        return false;
    }

    @Override
    protected PathInfo parsePathInfo(@NonNull DirectoryInode parent,
                                     @NonNull String path,
                                     @NonNull InodeType type) throws IOException {
        String p = PathUtils.formatPath(String.format("%s/%s", parent.getPath(), path));
        S3PathInfo pi = checkAndGetPath(parent);
        return new S3PathInfo(this, parent.getDomain(), pi.bucket(), p, type);
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        if (path instanceof S3PathInfo) {
            if (path.directory()) {
                return true;
            }
            try {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .bucket(((S3PathInfo) path).bucket())
                        .key(path.fsPath())
                        .build();
                HeadObjectResponse response = client.headObject(request);
                return (response != null);
            } catch (NoSuchKeyException nk) {
                return false;
            } catch (S3Exception ex) {
                throw new IOException(ex);
            }
        }
        return false;
    }

    @Override
    protected Reader getReader(@NonNull FileInode inode) throws IOException {
        S3PathInfo pi = (S3PathInfo) inode.getPathInfo();
        if (pi == null) {
            S3Container container = (S3Container) domainMap.get(inode.getDomain());
            pi = new S3PathInfo(this, inode, container.getBucket());
            inode.setPathInfo(pi);
        }
        if (!pi.exists()) {
            throw new IOException(String.format("S3 file not found. [path=%s]", inode.getPath()));
        }
        return new S3Reader(this, inode).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, boolean overwrite) throws IOException {
        S3PathInfo pi = (S3PathInfo) inode.getPathInfo();
        if (pi == null) {
            S3Container container = (S3Container) domainMap.get(inode.getDomain());
            pi = new S3PathInfo(this, inode, container.getBucket());
            inode.setPathInfo(pi);
        }
        return new S3Writer(inode, this, overwrite).open();
    }

    @Override
    protected Writer getWriter(@NonNull FileInode inode, @NonNull File temp) throws IOException {
        S3PathInfo pi = (S3PathInfo) inode.getPathInfo();
        if (pi == null) {
            S3Container container = (S3Container) domainMap.get(inode.getDomain());
            pi = new S3PathInfo(this, inode, container.getBucket());
            inode.setPathInfo(pi);
        }
        return new S3Writer(inode, this, temp).open();
    }

    @Override
    protected void doCopy(@NonNull FileInode source, @NonNull FileInode target) throws IOException {
        S3PathInfo sp = checkAndGetPath(source);
        S3PathInfo tp = checkAndGetPath(target);
        CopyObjectRequest cr = CopyObjectRequest.builder()
                .sourceBucket(sp.bucket())
                .sourceKey(sp.fsPath())
                .destinationBucket(tp.bucket())
                .destinationKey(tp.fsPath())
                .build();
        CopyObjectResponse rc = client.copyObject(cr);
    }

    @Override
    protected PathInfo renameFile(@NonNull FileInode source,
                                  @NonNull String name) throws IOException {
        S3PathInfo pi = checkAndGetPath(source);
        String path = String.format("%s/%s", pi.parent(), name);
        return new S3PathInfo(this, pi.domain(), pi.bucket(), path, InodeType.File);
    }

    @Override
    protected void doMove(@NonNull FileInode source,
                          @NonNull FileInode target) throws IOException {
        doCopy(source, target);
        S3PathInfo sp = checkAndGetPath(source);
        if (!delete(sp, false)) {
            throw new IOException(String.format("Failed to delete file. [path=%s]", sp.pathConfig()));
        }
    }

    protected File read(@NonNull FileInode path) throws IOException {
        try {
            return cache().get(path);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected long updateTime(@NonNull S3PathInfo path) throws IOException {
        ListObjectsRequest request = ListObjectsRequest.builder()
                .bucket(path.bucket())
                .prefix(path.path())
                .build();
        ListObjectsResponse response = client.listObjects(request);
        if (response.hasContents()) {
            for (S3Object obj : response.contents()) {
                if (obj.key().compareTo(path.path()) == 0) {
                    return obj.lastModified().toEpochMilli();
                }
            }
        }
        throw new IOException(String.format("S3 Object not found. [bucket=%s][path=%s]",
                path.bucket(), path.path()));
    }

    @Override
    public PathInfo createSubPath(@NonNull PathInfo parent, @NonNull String path) {
        Preconditions.checkArgument(parent instanceof S3PathInfo);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        return new S3PathInfo(this, parent.domain(), ((S3PathInfo) parent).bucket(), path, InodeType.Directory);
    }

    @Override
    public PathInfo createPath(@NonNull String domain,
                               @NonNull Container container,
                               @NonNull String path,
                               @NonNull InodeType type) {
        Preconditions.checkArgument(container instanceof S3Container);
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        path = PathUtils.formatPath(path);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        return new S3PathInfo(this, domain, ((S3Container) container).getBucket(), path, type);
    }

    @Override
    public FileInode upload(@NonNull File source,
                            @NonNull FileInode inode,
                            boolean clearLock) throws IOException {
        S3PathInfo path = checkAndGetPath(inode);
        S3FileUploader task = new S3FileUploader(this, client, inode, source, this, clearLock);
        uploader.submit(task);
        return inode;
    }

    @Override
    public File download(@NonNull FileInode inode, long timeout) throws IOException {
        S3PathInfo path = checkAndGetPath(inode);
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
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.fsPath())
                    .build();
            try (FileOutputStream fos = new FileOutputStream(path.temp())) {
                client.getObject(request, ResponseTransformer.toOutputStream(fos));
            }
            return path.temp();
        }
        return null;
    }

    @Override
    public long size(@NonNull PathInfo path) throws IOException {
        if (path instanceof S3PathInfo) {
            try {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .key(((S3PathInfo) path).bucket())
                        .bucket(path.fsPath())
                        .build();
                HeadObjectResponse response = client.headObject(request);
                return response.contentLength();
            } catch (NoSuchKeyException nk) {
                throw new IOException(
                        String.format("File not found. [bucket=%s, path=%s]",
                                ((S3PathInfo) path).bucket(), path.path()));
            } catch (S3Exception ex) {
                throw new IOException(ex);
            }
        }
        throw new IOException(String.format("Invalid Path handle. [type=%s]", path.getClass().getCanonicalName()));
    }

    @Override
    public void onSuccess(@NonNull FileInode inode,
                          @NonNull Object response,
                          boolean clearLock) {
        Preconditions.checkArgument(response instanceof HeadObjectResponse);
        try {
            inode.setSyncedSize(((HeadObjectResponse) response).contentLength());
            inode.setSyncTimestamp(((HeadObjectResponse) response).lastModified().toEpochMilli());
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

    @Getter
    @Accessors(fluent = true)
    public static class S3FileSystemConfigReader extends RemoteFileSystemConfigReader {
        public S3FileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, S3FileSystemSettings.class, S3Container.class);
        }
    }

    public static class S3FileUploader extends FileUploader {
        private final S3Client client;
        private final File source;

        protected S3FileUploader(@NonNull RemoteFileSystem fs,
                                 @NonNull S3Client client,
                                 @NonNull FileInode inode,
                                 @NonNull File source,
                                 @NonNull FileUploadCallback callback,
                                 boolean clearLock) {
            super(fs, inode, callback, clearLock);
            this.client = client;
            this.source = source;
        }

        @Override
        protected Object upload() throws Throwable {
            S3PathInfo pi = fs.checkAndGetPath(inode);
            if (!source.exists()) {
                throw new IOException(String.format("Source file not found. [path=%s]", source.getAbsolutePath()));
            }
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(pi.bucket())
                    .key(pi.fsPath())
                    .build();
            PutObjectResponse response = client
                    .putObject(request, RequestBody.fromFile(source));
            S3Waiter waiter = client.waiter();
            HeadObjectRequest requestWait = HeadObjectRequest.builder()
                    .bucket(pi.bucket())
                    .key(pi.fsPath())
                    .build();

            WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);
            if (waiterResponse.matched().response().isEmpty()) {
                throw new Exception("Failed to get valid response...");
            }
            return waiterResponse.matched().response().get();
        }
    }
}
