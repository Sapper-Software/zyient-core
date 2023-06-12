package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.core.io.Archiver;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class ArchivePathInfo {
    public static final String CONFIG_KEY_PREFIX_SOURCE = "source.";
    public static final String CONFIG_KEY_COMPRESSED = "compressed";

    private final Archiver archiver;
    private final String domain;
    private final String uuid;
    private final Map<String, String> source;
    private String path;
    private boolean compressed;

    protected ArchivePathInfo(@NonNull Archiver archiver,
                              @NonNull PathInfo path) {
        this.archiver = archiver;
        this.uuid = path.uuid();
        this.path = path.path();
        this.domain = path.domain();
        this.source = path.pathConfig();
        this.compressed = false;
    }

    protected ArchivePathInfo(@NonNull Archiver archiver,
                              @NonNull Map<String, String> config) throws IOException {
        this.archiver = archiver;
        domain = checkKey(PathInfo.CONFIG_KEY_DOMAIN, config);
        path = checkKey(PathInfo.CONFIG_KEY_PATH, config);
        uuid = checkKey(PathInfo.CONFIG_KEY_UUID, config);
        if (config.containsKey(CONFIG_KEY_COMPRESSED)) {
            compressed = Boolean.parseBoolean(config.get(CONFIG_KEY_COMPRESSED));
        } else {
            compressed = false;
        }
        source = new HashMap<>();
        for (String key : config.keySet()) {
            if (key.startsWith(CONFIG_KEY_PREFIX_SOURCE)) {
                source.put(key, config.get(key));
            }
        }
        if (source.isEmpty()) {
            throw new IOException("Source path config not set.");
        }
    }

    public Map<String, String> pathConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(PathInfo.CONFIG_KEY_DOMAIN, domain);
        config.put(PathInfo.CONFIG_KEY_PATH, path);
        config.put(PathInfo.CONFIG_KEY_UUID, uuid);
        config.put(CONFIG_KEY_COMPRESSED, String.valueOf(compressed));
        for (String key : source.keySet()) {
            String k = String.format("%s%s", CONFIG_KEY_PREFIX_SOURCE, key);
            source.put(k, source.get(key));
        }
        return config;
    }

    protected String checkKey(@NonNull String key,
                              @NonNull Map<String, String> values) throws IOException {
        String value = values.get(key);
        if (Strings.isNullOrEmpty(value)) {
            throw new IOException(String.format("Config Key not found. [key=%s]", key));
        }
        return value;
    }
}
