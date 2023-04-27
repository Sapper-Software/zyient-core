package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.io.impl.local.LocalFileSystem;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class RemoteFileSystem extends LocalFileSystem {
    private Map<String, String> bucketMap = new HashMap<>();

    /**
     * @param config
     * @param pathPrefix
     * @param keyStore
     * @return
     * @throws IOException
     */
    @Override
    public CDCFileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              String pathPrefix,
                              KeyStore keyStore) throws IOException {
        Preconditions.checkNotNull(fsConfig());
        Preconditions.checkState(fsConfig() instanceof RemoteFileSystemConfig);
        super.init(config, pathPrefix, keyStore);
        if (((RemoteFileSystemConfig) fsConfig()).mappings != null) {
            bucketMap = ((RemoteFileSystemConfig) fsConfig()).mappings;
        }

        File tdir = new File(fsConfig().tempDir());
        if (!tdir.exists()) {
            tdir.mkdirs();
        } else {
            FileUtils.deleteDirectory(tdir);
            tdir.mkdirs();
        }
        return this;
    }

    public void debug(Object mesg) {
        DefaultLogger.LOGGER.debug(String.format("RESPONSE: %s", mesg));
    }

    @Getter
    @Accessors(fluent = true)
    public static class RemoteFileSystemConfig extends FileSystemConfig {
        public static final String CONFIG_DOMAIN_MAP = "domains.mapping";

        private Map<String, String> mappings;

        public RemoteFileSystemConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                      @NonNull String path) {
            super(config, path);
        }

        @Override
        public void read(@NonNull Class<? extends ConfigReader> type) throws ConfigurationException {
            super.read(type);
            if (checkIfNodeExists(get(), CONFIG_DOMAIN_MAP)) {
                mappings = readAsMap(get(), CONFIG_DOMAIN_MAP);
            }
        }
    }
}
