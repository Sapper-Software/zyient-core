package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.FileUploadCallback;
import ai.sapper.cdc.core.io.impl.RemoteFileSystem;
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
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class S3FileSystem extends RemoteFileSystem {

    @Getter(AccessLevel.PACKAGE)
    private S3Client client;
    private S3FileSystemSettings settings;

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
    public FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                           @NonNull BaseEnv<?> env) throws IOException {
        try {
            super.init(config, env, new S3FileSystemConfigReader(config));
            settings = (S3FileSystemSettings) configReader().settings();
            if (client == null) {
                Region region = Region.of(settings.region);
                client = S3Client.builder()
                        .region(region)
                        .build();
            }
            return postInit();
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.error(LOG, "Error initializing Local FileSystem.", t);
            state().error(t);
            throw new IOException(t);
        }
    }

    @Override
    public FileSystem init(@NonNull FileSystemSettings settings, @NonNull BaseEnv<?> env) throws IOException {
        Preconditions.checkArgument(settings instanceof S3FileSystemSettings);
        super.init(settings, env);
        try {
            this.settings = (S3FileSystemSettings) settings;
            if (client == null) {
                Region region = Region.of(this.settings.region);
                client = S3Client.builder()
                        .region(region)
                        .build();
            }
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
        return new S3PathInfo(this, values);
    }

    @Override
    public DirectoryInode mkdir(@NonNull DirectoryInode parent, @NonNull String name) throws IOException {
        String path = PathUtils.formatPath(String.format("%s/%s", parent.getAbsolutePath(), name));
        S3PathInfo pp = (S3PathInfo) parsePathInfo(parent.getPath());
        S3PathInfo pi = new S3PathInfo(this, pp.domain(), pp.bucket(), path);
        Inode node = createInode(InodeType.Directory, pi);
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInode(node, pi);
    }

    @Override
    public DirectoryInode mkdirs(@NonNull String domain, @NonNull String path) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        path = getAbsolutePath(path, domain);
        Container container = domainMap.get(domain);
        if (!(container instanceof S3Container)) {
            throw new IOException(String.format("Mapped container not found. [domain=%s]", domain));
        }

        S3PathInfo pi = new S3PathInfo(this, domain, ((S3Container) container).getBucket(), path);
        Inode node = createInode(InodeType.Directory, pi);

        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (DirectoryInode) updateInode(node, pi);
    }

    @Override
    public FileInode create(@NonNull String domain, @NonNull String path) throws IOException {
        path = getAbsolutePath(path, domain);
        Container container = domainMap.get(domain);
        if (!(container instanceof S3Container)) {
            throw new IOException(String.format("Mapped container not found. [domain=%s]", domain));
        }
        S3PathInfo pi = new S3PathInfo(this, domain, ((S3Container) container).getBucket(), path);
        return create(pi);
    }

    @Override
    public FileInode create(@NonNull PathInfo pathInfo) throws IOException {
        Preconditions.checkArgument(pathInfo instanceof S3PathInfo);
        S3PathInfo pi = (S3PathInfo) pathInfo;
        Inode node = createInode(InodeType.File, pi);
        if (node.getPath() == null)
            node.setPath(pi.pathConfig());
        if (node.getPathInfo() == null)
            node.setPathInfo(pi);
        return (FileInode) updateInode(node, pi);
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
                        DeleteObjectResponse dres = client.deleteObject(dr);
                        if (!dres.deleteMarker() && ret) {
                            ret = false;
                        }
                    }
                    return ret;
                } else {
                    DeleteObjectRequest dr = DeleteObjectRequest.builder()
                            .bucket(s3path.bucket())
                            .key(s3path.path())
                            .build();
                    client.deleteObject(dr);
                }
            }
        }
        return false;
    }

    @Override
    public boolean exists(@NonNull PathInfo path) throws IOException {
        if (path instanceof S3PathInfo) {
            try {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .key(((S3PathInfo) path).bucket())
                        .bucket(getAbsolutePath(path.path(), path.domain()))
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
            throw new IOException(String.format("Local file not found. [path=%s]", inode.getAbsolutePath()));
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
    public FileInode upload(@NonNull File source,
                            @NonNull FileInode inode,
                            boolean clearLock) throws IOException {
        S3PathInfo path = (S3PathInfo) inode.getPathInfo();
        if (path == null) {
            throw new IOException(
                    String.format("Path information not set in inode. [domain=%s, path=%s]",
                            inode.getDomain(), inode.getAbsolutePath()));
        }
        S3FileUploader task = new S3FileUploader(this, client, inode, this, clearLock);
        uploader.submit(task);
        return inode;
    }

    @Override
    public File download(@NonNull FileInode inode) throws IOException {
        S3PathInfo path = (S3PathInfo) inode.getPathInfo();
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
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(path.bucket())
                    .key(path.path())
                    .build();
            try (FileOutputStream fos = new FileOutputStream(path.temp())) {
                client.getObject(request, ResponseTransformer.toOutputStream(fos));
            }
            return path.temp();
        }
        return null;
    }

    private File getInodeTempPath(S3PathInfo path) throws IOException {
        String dir = FilenameUtils.getPath(path.path());
        dir = PathUtils.formatPath(String.format("%s/%s", path.domain(), dir));
        String fname = FilenameUtils.getName(path.path());
        return createTmpFile(dir, fname);
    }

    @Override
    public long size(@NonNull PathInfo path) throws IOException {
        if (path instanceof S3PathInfo) {
            try {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .key(((S3PathInfo) path).bucket())
                        .bucket(getAbsolutePath(path.path(), path.domain()))
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
            if (clearLock) {
                inode.getState().state(EFileState.Synced);
                fileUnlock(inode);
            } else {
                inode.getState().state(EFileState.Updating);
                fileUpdateLock(inode);
            }
            updateInode(inode, inode.getPathInfo());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(LOG, ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void onError(@NonNull FileInode inode, @NonNull Throwable error) {
        try {
            String temp = null;
            if (inode.getLock() != null) {
                temp = inode.getLock().getLocalPath();
            }
            DefaultLogger.error(LOG,
                    String.format("Error uploading file. [domain=%s][path=%s][temp=%s][error=%s]",
                            inode.getDomain(),
                            inode.getAbsolutePath(),
                            temp,
                            error.getLocalizedMessage()));
            DefaultLogger.stacktrace(error);
            inode.getState().error(error);
            updateInode(inode, inode.getPathInfo());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(LOG, ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }

    @Getter
    @Setter
    public static class S3FileSystemSettings extends RemoteFileSystemSettings {
        public static final String CONFIG_REGION = "region";

        @Config(name = CONFIG_REGION)
        private String region;

        public S3FileSystemSettings() {
            type(S3FileSystem.class.getCanonicalName());
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class S3FileSystemConfigReader extends RemoteFileSystemConfigReader {
        public static final String __CONFIG_PATH = "s3";

        public S3FileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, S3FileSystemSettings.class, S3Container.class);
        }
    }

    public static class S3FileUploader extends FileUploader {
        private final S3Client client;

        protected S3FileUploader(@NonNull RemoteFileSystem fs,
                                 @NonNull S3Client client,
                                 @NonNull FileInode inode,
                                 @NonNull FileUploadCallback callback,
                                 boolean clearLock) {
            super(fs, inode, callback, clearLock);
            this.client = client;
        }

        @Override
        protected Object upload() throws Throwable {
            S3PathInfo pi = (S3PathInfo) inode.getPathInfo();
            if (pi == null) {
                throw new Exception("S3 Path information not specified...");
            }
            File source = pi.temp();
            if (source == null) {
                throw new Exception("File to upload not specified...");
            }
            if (!source.exists()) {
                throw new IOException(String.format("Source file not found. [path=%s]", source.getAbsolutePath()));
            }
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(pi.bucket())
                    .key(pi.path())
                    .build();
            PutObjectResponse response = client
                    .putObject(request, RequestBody.fromFile(source));
            S3Waiter waiter = client.waiter();
            HeadObjectRequest requestWait = HeadObjectRequest.builder()
                    .bucket(pi.bucket())
                    .key(pi.path())
                    .build();

            WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);
            if (waiterResponse.matched().response().isEmpty()) {
                throw new Exception("Failed to get valid response...");
            }
            return waiterResponse.matched().response().get();
        }
    }
}
