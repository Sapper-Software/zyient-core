package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.Locale;

@Getter
@Accessors(fluent = true)
public class AzureFsClient {
    private BlobServiceClient client;
    private AzureFsClientConfig config;
    private AzureStorageAuth auth;

    @SuppressWarnings("unchecked")
    public AzureFsClient init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull KeyStore keyStore) throws IOException {
        try {
            config = new AzureFsClientConfig(xmlConfig);
            config.read(AzureFsClientConfig.class);
            Class<? extends AzureStorageAuth> ac = (Class<? extends AzureStorageAuth>) Class.forName(config.authClass);
            auth = ac.getDeclaredConstructor()
                    .newInstance()
                    .withAccount(config().authAccount)
                    .init(config.config(), keyStore);
            String endpoint = String.format(Locale.ROOT, config().endpointUrl, config.authAccount);
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .endpoint(endpoint);
            client = auth.credentials(builder).buildClient();
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public BlobContainerClient getContainer() throws IOException {
        if (client == null) {
            throw new IOException("Storage account not initialized...");
        }
        return client.getBlobContainerClient(config.container);
    }

    @Getter
    @Setter
    public static class AzureFsClientConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "client";

        @Config(name = "endpointUrl")
        private String endpointUrl;
        @Config(name = "authClass")
        private String authClass;
        @Config(name = "account")
        private String authAccount;
        @Config(name = "container")
        private String container;

        public AzureFsClientConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
