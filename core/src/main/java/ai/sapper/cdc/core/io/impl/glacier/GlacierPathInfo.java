package ai.sapper.cdc.core.io.impl.glacier;

import ai.sapper.cdc.core.io.model.ArchivePathInfo;
import ai.sapper.cdc.core.io.model.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.services.glacier.GlacierClient;

import java.io.IOException;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class GlacierPathInfo extends ArchivePathInfo {
    public static final String CONFIG_KEY_VAULT = "vault";
    private final GlacierClient client;
    private final String vault;


    protected GlacierPathInfo(@NonNull GlacierClient client,
                              @NonNull String path,
                              @NonNull String domain,
                              @NonNull Map<String, String> source,
                              @NonNull String vault) {
        super(domain, source, path);
        this.client = client;
        this.vault = vault;
    }

    protected GlacierPathInfo(@NonNull GlacierClient client,
                              @NonNull Map<String, String> config) throws IOException {
        super(config);
        this.client = client;
        this.vault = checkKey(CONFIG_KEY_VAULT, config);
    }

    @Override
    public Map<String, String> pathConfig() {
        Map<String, String> config =  super.pathConfig();
        config.put(CONFIG_KEY_VAULT, vault);
        return config;
    }
}
