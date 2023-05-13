package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.impl.local.LocalPathInfo;
import ai.sapper.cdc.core.io.model.Inode;
import com.azure.storage.blob.BlobClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

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

    protected AzurePathInfo(@NonNull FileSystem fs,
                         @NonNull Inode node,
                         @NonNull String container) {
        super(fs, node);
        Preconditions.checkArgument(fs instanceof AzureFileSystem);
        client = ((AzureFileSystem) fs).client();
        this.container = container;
    }

    protected AzurePathInfo(@NonNull FileSystem fs,
                         @NonNull String domain,
                         @NonNull String container,
                         @NonNull String path) {
        super(fs, path, domain);
        Preconditions.checkArgument(fs instanceof AzureFileSystem);
        client = ((AzureFileSystem) fs).client();
        this.container = container;
    }

    protected AzurePathInfo(@NonNull FileSystem fs,
                         @NonNull Map<String, String> config) {
        super(fs, config);
        Preconditions.checkArgument(fs instanceof AzureFileSystem);
        client = ((AzureFileSystem) fs).client();
        container = config.get(CONFIG_KEY_BUCKET);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(container));
    }

    protected AzurePathInfo withTemp(@NonNull File temp) {
        this.temp = temp;
        return this;
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean exists() throws IOException {
        try {
            BlobClient c = client.getContainer(container).getBlobClient(path());
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
            Preconditions.checkArgument(fs() instanceof AzureFileSystem);
            dataSize(((AzureFileSystem) fs()).size(this));
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
