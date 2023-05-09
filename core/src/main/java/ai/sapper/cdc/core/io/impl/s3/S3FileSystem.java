package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.io.model.Container;
import ai.sapper.cdc.core.io.model.DirectoryInode;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import ai.sapper.cdc.core.io.impl.RemoteFileSystem;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class S3FileSystem extends RemoteFileSystem {

    @Getter(AccessLevel.PACKAGE)
    private S3Client client;

    public S3FileSystem withClient(@NonNull S3Client client) {
        this.client = client;
        return this;
    }

    private String findBucket(String domain) {
        if (bucketMap().containsKey(domain)) {
            return bucketMap().get(domain);
        }
        return defaultBucket;
    }

    /**
     * @param config
     * @param pathPrefix
     * @return
     * @throws IOException
     */
    @Override
    public CDCFileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String pathPrefix,
                              KeyStore keyStore) throws IOException {
        try {
            fsConfig = new S3FileSystemConfig(config, pathPrefix);
            fsConfig.read(S3FileSystemConfig.class);
            fsConfig(fsConfig);
            super.init(config, pathPrefix, keyStore);

            this.defaultBucket = fsConfig.defaultBucket;
            if (client == null) {
                Region region = Region.of(fsConfig.region);
                client = S3Client.builder()
                        .region(region)
                        .build();
            }

            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * @param path
     * @param domain
     * @return
     * @throws IOException
     */
    @Override
    public PathInfo get(@NonNull String path, String domain) throws IOException {
        try {
            String bucket = findBucket(domain);
            if (root() != null) {
                path = PathUtils.formatPath(String.format("%s/%s/%s", root().path(), domain, path));
            }
            return new S3PathInfo(this.client, domain, bucket, path);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * @param config
     * @return
     */
    @Override
    public PathInfo get(@NonNull Map<String, String> config) {
        try {
            return new S3PathInfo(this.client, config);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static S3PathInfo checkPath(PathInfo pathInfo) throws IOException {
        if (!(pathInfo instanceof S3PathInfo)) {
            throw new IOException(
                    String.format("Invalid Path type. [type=%s]", pathInfo.getClass().getCanonicalName()));
        }
        return (S3PathInfo) pathInfo;
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

    /**
     * @param path
     * @param name
     * @return
     * @throws IOException
     */
    @Override
    public String mkdir(@NonNull PathInfo path, @NonNull String name) throws IOException {
        checkPath(path);
        File f = new File(PathUtils.formatPath(String.format("%s/%s", path.path(), name)));
        return f.getAbsolutePath();
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public String mkdirs(@NonNull PathInfo path) throws IOException {
        checkPath(path);
        File f = new File(PathUtils.formatPath(path.path()));
        return f.getAbsolutePath();
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        S3PathInfo s3path = checkPath(path);
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
        return false;
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public List<String> list(@NonNull PathInfo path, boolean recursive) throws IOException {
        S3PathInfo s3path = checkPath(path);
        if (bucketExists(s3path.bucket())) {
            ListObjectsRequest request = ListObjectsRequest
                    .builder()
                    .bucket(s3path.bucket())
                    .prefix(path.path())
                    .build();

            ListObjectsResponse res = client.listObjects(request);
            List<S3Object> objects = res.contents();
            List<String> paths = new ArrayList<>();
            for (S3Object obj : objects) {
                if (recursive) {
                    paths.add(obj.key());
                } else {
                    String p = obj.key();
                    p = p.replaceFirst(path.path(), "");
                    if (p.startsWith("/")) {
                        p = p.substring(1);
                    }
                    if (p.indexOf('/') < 0) {
                        paths.add(obj.key());
                    }
                }
            }
            if (!paths.isEmpty()) return paths;
        }
        return null;
    }

    /**
     * @param path
     * @param dirQuery
     * @param fileQuery
     * @return
     * @throws IOException
     */
    @Override
    public List<String> find(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException {
        List<String> paths = list(path, true);
        if (paths != null && !paths.isEmpty()) {
            Pattern dp = null;
            if (!Strings.isNullOrEmpty(dirQuery)) {
                dp = Pattern.compile(dirQuery);
            }
            Pattern fp = Pattern.compile(fileQuery);
            List<String> out = new ArrayList<>();
            for (String p : paths) {
                String fname = FilenameUtils.getName(p);
                String dir = FilenameUtils.getFullPath(p);
                Matcher dm = null;
                if (dp != null) {
                    dm = dp.matcher(dir);
                }
                Matcher fm = fp.matcher(fname);
                if (dm == null || dm.matches()) {
                    if (fm.matches()) {
                        out.add(p);
                    }
                }
            }
            if (!out.isEmpty()) return out;
        }
        return null;
    }

    /**
     * @param path
     * @param dirQuery
     * @param fileQuery
     * @return
     * @throws IOException
     */
    @Override
    public List<String> findFiles(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException {
        return find(path, dirQuery, fileQuery);
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
        Preconditions.checkArgument(path instanceof S3PathInfo);
        return new S3Writer(path, this).open(overwrite);
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Reader reader(@NonNull PathInfo path) throws IOException {
        Preconditions.checkArgument(path instanceof S3PathInfo);
        return new S3Reader(this, path).open();
    }

    /**
     * @param path
     * @param domain
     * @param prefix
     * @return
     * @throws IOException
     */
    @Override
    public PathInfo get(@NonNull String path,
                        String domain,
                        boolean prefix) throws IOException {
        try {
            if (prefix) {
                return get(path, domain);
            }
            String bucket = findBucket(domain);
            return new S3PathInfo(client, domain, bucket, path);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected S3PathInfo read(@NonNull S3PathInfo path) throws IOException {
        try {
            return cache.get(path);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected void download(@NonNull S3PathInfo path) throws IOException {
        if (path.temp().exists()) {
            path.temp().delete();
        }
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(path.bucket())
                .key(path.path())
                .build();
        try (FileOutputStream fos = new FileOutputStream(path.temp())) {
            client.getObject(request, ResponseTransformer.toOutputStream(fos));
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

    /**
     * @param source
     * @param directory
     * @throws IOException
     */
    @Override
    public PathInfo upload(@NonNull File source, @NonNull PathInfo directory) throws IOException {
        try {
            S3PathInfo s3dir = (S3PathInfo) directory;
            String path = String.format("%s/%s", s3dir.path(), FilenameUtils.getName(source.getAbsolutePath()));
            PathInfo dest = get(path, directory.domain(), false);
            S3PathInfo s3path = checkPath(dest);
            if (s3path.exists()) {
                delete(s3path);
            }
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(s3path.bucket())
                    .key(s3path.path())
                    .build();
            PutObjectResponse response = client()
                    .putObject(request, RequestBody.fromFile(source));
            S3Waiter waiter = client().waiter();
            HeadObjectRequest requestWait = HeadObjectRequest.builder()
                    .bucket(s3path.bucket())
                    .key(s3path.path())
                    .build();

            WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);

            waiterResponse.matched().response().ifPresent(this::debug);

            s3path.withTemp(source);
            cache.put(s3path, System.currentTimeMillis());

            return s3path;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public FileInode upload(@NonNull File source, @NonNull DirectoryInode directory) throws IOException {
        return null;
    }

    @Override
    public File download(@NonNull FileInode inode) throws IOException {
        return null;
    }

    @Getter
    @Setter
    public static class S3FileSystemSettings extends FileSystemSettings {
        public static final String CONFIG_REGION = "region";

        @Config(name = CONFIG_REGION)
        private String region;
    }

    @Getter
    @Accessors(fluent = true)
    public static class S3FileSystemConfigReader extends FileSystemConfigReader {
        public static final String __CONFIG_PATH = "s3";

        public S3FileSystemConfigReader(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                        @NonNull Class<? extends Container> containerType) {
            super(config, __CONFIG_PATH, S3FileSystemSettings.class, containerType);
        }
    }
}
