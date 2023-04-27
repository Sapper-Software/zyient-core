package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.utils.ChecksumUtils;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.io.impl.local.LocalPathInfo;
import ai.sapper.cdc.core.io.impl.s3.S3FileSystem;
import com.azure.storage.blob.BlobClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class AzurePathInfo extends LocalPathInfo {
    public static final String CONFIG_KEY_BUCKET = "container";

    private final AzureFsClient client;
    private final String container;
    private File temp;
    private final boolean directory;

    protected AzurePathInfo(@NonNull AzureFsClient client,
                            @NonNull String domain,
                            @NonNull String container,
                            @NonNull String path) throws Exception {
        super(path, domain);
        this.client = client;
        this.container = container;
        init();
        directory = false;
    }

    protected AzurePathInfo(@NonNull AzureFsClient client,
                            @NonNull String domain,
                            @NonNull String container,
                            @NonNull String path,
                            boolean directory) throws Exception {
        super(path, domain);
        this.client = client;
        this.container = container;
        if (!directory)
            init();
        this.directory = directory;
    }

    protected AzurePathInfo(@NonNull AzureFsClient client,
                            @NonNull Map<String, String> config) throws Exception {
        super(config);
        this.client = client;
        container = config.get(CONFIG_KEY_BUCKET);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(container));
        init();
        directory = false;
    }

    private void init() throws Exception {
        String name = FilenameUtils.getName(path());
        if (!Strings.isNullOrEmpty(name)) {
            String hash = ChecksumUtils.generateHash(path());
            String tempf = String.format("%s/%s/%s/%s",
                    S3FileSystem.TEMP_PATH,
                    container,
                    hash,
                    name);
            temp = new File(tempf);
            file(temp);
        }
    }

    protected AzurePathInfo withTemp(@NonNull File temp) {
        this.temp = temp;
        file(temp);

        return this;
    }

    /**
     * @return
     */
    @Override
    public PathInfo parentPathInfo() throws Exception {
        String path = path();
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 2);
        }
        return new AzurePathInfo(client, domain(), container, FilenameUtils.getFullPath(path), true);
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean isDirectory() throws IOException {
        if (directory) return true;
        return path().endsWith("/");
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean isFile() throws IOException {
        return exists();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean exists() throws IOException {
        try {
            BlobClient c = client.getContainer().getBlobClient(path());
            return c.exists();
        } catch (Exception ex) {
            return false;
        }
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
            dataSize(0);
        }
        return dataSize();
    }

    /**
     * @return
     */
    @Override
    public Map<String, String> pathConfig() {
        Map<String, String> config = super.pathConfig();
        config.put(CONFIG_KEY_BUCKET, container);
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
        return "{container=" + container + ", path=" + path() + " [domain=" + domain() + "]}";
    }
}
