package ai.sapper.cdc.core.io.model;

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
    public static final String CONFIG_KEY_DOMAIN = "domain";
    public static final String CONFIG_KEY_PREFIX_SOURCE = "source.";
    public static final String CONFIG_KEY_PATH = "path";

    private final String domain;
    private final Map<String, String> source;
    private String path;

    protected ArchivePathInfo(@NonNull String domain,
                              @NonNull Map<String, String> source) {
        this.domain = domain;
        this.source = source;
    }

    protected ArchivePathInfo(@NonNull String domain,
                              @NonNull Map<String, String> source,
                              @NonNull String path) {
        this.domain = domain;
        this.source = source;
        this.path = path;
    }

    protected ArchivePathInfo(@NonNull Map<String, String> config) throws IOException {
        domain = checkKey(CONFIG_KEY_DOMAIN, config);
        path = checkKey(CONFIG_KEY_PATH, config);
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
        config.put(CONFIG_KEY_DOMAIN, domain);
        config.put(CONFIG_KEY_PATH, path);
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
