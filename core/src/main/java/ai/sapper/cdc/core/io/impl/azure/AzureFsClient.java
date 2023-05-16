package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.common.base.Preconditions;
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
    private AzureFsClientSettings settings;
    private AzureStorageAuth auth;

    @SuppressWarnings("unchecked")
    public AzureFsClient init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull KeyStore keyStore) throws IOException {
        try {
            ConfigReader config = new ConfigReader(xmlConfig, AzureFsClientSettings.__CONFIG_PATH, AzureFsClientSettings.class);
            config.read();
            settings = (AzureFsClientSettings) config.settings();

            Class<? extends AzureStorageAuth> ac = (Class<? extends AzureStorageAuth>) Class.forName(settings.authClass);
            auth = ac.getDeclaredConstructor()
                    .newInstance()
                    .withAccount(settings.authAccount)
                    .init(config.config(), keyStore);

            String endpoint = String.format(Locale.ROOT, settings.endpointUrl, settings.authAccount);
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .endpoint(endpoint);
            client = auth.credentials(builder).buildClient();
            settings.authSettings = auth.settings();

            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public AzureFsClient init(@NonNull AzureFsClientSettings settings,
                              @NonNull KeyStore keyStore) throws IOException {
        Preconditions.checkNotNull(settings.authSettings);
        try {
            this.settings = settings;

            Class<? extends AzureStorageAuth> ac = (Class<? extends AzureStorageAuth>) Class.forName(settings.authClass);
            auth = ac.getDeclaredConstructor()
                    .newInstance()
                    .withAccount(settings.authAccount)
                    .init(settings.authSettings, keyStore);

            String endpoint = String.format(Locale.ROOT, settings.endpointUrl, settings.authAccount);
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .endpoint(endpoint);
            client = auth.credentials(builder).buildClient();

            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public BlobContainerClient getContainer(@NonNull String container) throws IOException {
        if (client == null) {
            throw new IOException("Storage account not initialized...");
        }
        return client.getBlobContainerClient(container);
    }

    @Getter
    @Setter
    public static class AzureFsClientSettings extends Settings {
        public static final String __CONFIG_PATH = "client";

        @Config(name = "endpointUrl")
        private String endpointUrl;
        @Config(name = "authClass")
        private String authClass;
        @Config(name = "account")
        private String authAccount;
        private AzureStorageAuth.AzureStorageAuthSettings authSettings;
    }
}
