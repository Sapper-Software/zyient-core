package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class PathInfo {
    public static final String CONFIG_KEY_TYPE = "type";
    public static final String CONFIG_KEY_DOMAIN = "domain";
    public static final String CONFIG_KEY_PATH = "path";

    private final FileSystem fs;
    private final String domain;
    private final String path;
    protected boolean directory;
    private long dataSize = -1;

    protected PathInfo(@NonNull FileSystem fs,
                       @NonNull Inode node) {
        this.fs = fs;
        this.directory = node.isDirectory();
        domain = node.getPath().get(CONFIG_KEY_DOMAIN);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
        path = node.getPath().get(CONFIG_KEY_PATH);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
    }

    protected PathInfo(@NonNull FileSystem fs,
                       @NonNull String path,
                       @NonNull String domain) {
        this.fs = fs;
        this.path = PathUtils.formatPath(path);
        this.domain = domain;
    }

    protected PathInfo(@NonNull FileSystem fs,
                       @NonNull Map<String, String> config) {
        this.fs = fs;
        domain = config.get(CONFIG_KEY_DOMAIN);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
        path = config.get(CONFIG_KEY_PATH);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
    }

    public String parent() {
        return FilenameUtils.getFullPath(path);
    }


    public String name() {
        return FilenameUtils.getName(path);
    }

    public String extension() {
        return FilenameUtils.getExtension(path);
    }

    public abstract boolean exists() throws IOException;

    public abstract long size() throws IOException;

    public Map<String, String> pathConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(CONFIG_KEY_TYPE, getClass().getCanonicalName());
        config.put(CONFIG_KEY_DOMAIN, domain);
        config.put(CONFIG_KEY_PATH, path);

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
        return "{domain=" + domain + ", path=" + path + "}";
    }
}
