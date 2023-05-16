package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

public interface AzureStorageAuth {
    AzureStorageAuth withAccount(@NonNull String account);

    AzureStorageAuth init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                          @NonNull KeyStore keyStore) throws IOException;

    AzureStorageAuth init(@NonNull AzureStorageAuthSettings setting,
                          @NonNull KeyStore keyStore) throws IOException;

    AzureStorageAuthSettings settings();

    BlobServiceClientBuilder credentials(@NonNull BlobServiceClientBuilder builder) throws Exception;

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    class AzureStorageAuthSettings extends Settings {
        public static final String __CONFIG_PATH = "auth";
    }
}
