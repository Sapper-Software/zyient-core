package ai.sapper.cdc.core.io.impl.glacier;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.common.utils.ChecksumUtils;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.ArchivePathInfo;
import ai.sapper.cdc.core.io.model.PathInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.glacier.model.UploadArchiveRequest;
import software.amazon.awssdk.services.glacier.model.UploadArchiveResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class GlacierArchiver extends Archiver {
    private GlacierClient client;
    private ConfigReader configReader;
    private GlacierArchiverSettings settings;

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig, String pathPrefix) throws IOException {
        try {
            if (Strings.isNullOrEmpty(pathPrefix)) {
                pathPrefix = Archiver.CONFIG_ARCHIVER;
            }
            configReader = new ConfigReader(xmlConfig, pathPrefix, GlacierArchiverSettings.class);
            configReader.read();
            settings = (GlacierArchiverSettings) configReader.settings();
            Region region = Region.of(settings.region);
            client = GlacierClient.builder()
                    .region(region)
                    .credentialsProvider(ProfileCredentialsProvider.create())
                    .build();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public ArchivePathInfo archive(@NonNull PathInfo source, @NonNull ArchivePathInfo target, @NonNull FileSystem sourceFS) throws IOException {
        Preconditions.checkArgument(target instanceof GlacierPathInfo);
        File zip = new File(source.path());
        try {
            String checkVal = ChecksumUtils.computeSHA256(zip);

            UploadArchiveRequest uploadRequest = UploadArchiveRequest.builder()
                    .vaultName(((GlacierPathInfo) target).vault())
                    .checksum(checkVal)
                    .build();

            UploadArchiveResponse res = client.uploadArchive(uploadRequest, Paths.get(zip.getAbsolutePath()));
            String id = res.archiveId();
            return new GlacierPathInfo(client, id, target.domain(), source.pathConfig(), ((GlacierPathInfo) target).vault());
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public File getFromArchive(@NonNull ArchivePathInfo path) throws IOException {
        
        throw new IOException("Method not implemented...");
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class GlacierArchiverSettings extends Settings {
        public static class Constants {
            public static final String CONFIG_REGION = "region";
            public static final String CONFIG_DEFAULT_VAULT = "defaultVault";
            public static final String CONFIG_DOMAIN_MAP = "domains.mapping";
        }

        @Config(name = Constants.CONFIG_REGION)
        private String region;
        @Config(name = Constants.CONFIG_DEFAULT_VAULT)
        private String defaultVault;
        @Config(name = Constants.CONFIG_DOMAIN_MAP, required = false, type = Map.class)
        private Map<String, String> mappings;
    }
}
