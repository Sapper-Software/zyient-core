package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.Inode;
import ai.sapper.cdc.core.io.model.PathInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class S3PathInfo extends PathInfo {
    public static final String CONFIG_KEY_BUCKET = "bucket";

    private final S3Client client;
    private final String bucket;
    private File temp;

    public S3PathInfo(@NonNull FileSystem fs,
                      @NonNull Inode node,
                      String bucket) {
        super(fs, node);
        Preconditions.checkArgument(fs instanceof S3FileSystem);
        this.client = ((S3FileSystem) fs).client();
        this.bucket = bucket;
    }

    protected S3PathInfo(@NonNull FileSystem fs,
                         @NonNull String domain,
                         @NonNull String bucket,
                         @NonNull String path) {
        super(fs, path, domain);
        Preconditions.checkArgument(fs instanceof S3FileSystem);
        this.client = ((S3FileSystem) fs).client();
        this.bucket = bucket;
        directory = false;
    }

    protected S3PathInfo(@NonNull FileSystem fs,
                         @NonNull String domain,
                         @NonNull String bucket,
                         @NonNull String path,
                         boolean directory) {
        super(fs, path, domain);
        Preconditions.checkArgument(fs instanceof S3FileSystem);
        this.client = ((S3FileSystem) fs).client();
        this.bucket = bucket;
        this.directory = directory;
    }

    protected S3PathInfo(@NonNull FileSystem fs,
                         @NonNull Map<String, String> config) {
        super(fs, config);
        Preconditions.checkArgument(fs instanceof S3FileSystem);
        this.client = ((S3FileSystem) fs).client();
        bucket = config.get(CONFIG_KEY_BUCKET);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(bucket));
        directory = false;
    }

    protected S3PathInfo withTemp(@NonNull File temp) {
        this.temp = temp;
        return this;
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean exists() throws IOException {
        if (path().endsWith("/")) return true;
        return fs().exists(this);
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public long size() throws IOException {
        if (temp.exists()) {
            Path p = Paths.get(temp.toURI());
            dataSize(Files.size(p));
        } else {
            S3FileSystem sfs = (S3FileSystem) fs();
            dataSize(sfs.size(this));
        }
        return dataSize();
    }

    /**
     * @return
     */
    @Override
    public Map<String, String> pathConfig() {
        Map<String, String> config = super.pathConfig();
        config.put(CONFIG_KEY_BUCKET, bucket);
        return config;
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return "{bucket=" + bucket + ", path=" + path() + " [domain=" + domain() + "]}";
    }
}
