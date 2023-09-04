/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.base.core.io.impl.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.keystore.KeyStore;
import lombok.Getter;
import lombok.NonNull;
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

            Class<? extends AzureStorageAuth> ac = (Class<? extends AzureStorageAuth>) Class.forName(settings.getAuthClass());
            auth = ac.getDeclaredConstructor()
                    .newInstance()
                    .withAccount(settings.getAuthAccount())
                    .init(config.config(), keyStore);

            String endpoint = String.format(Locale.ROOT, settings.getEndpointUrl(), settings.getAuthAccount());
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .endpoint(endpoint);
            client = auth.credentials(builder).buildClient();
            settings.setAuthSettings(auth.settings());

            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public AzureFsClient init(@NonNull AzureFsClientSettings settings,
                              @NonNull KeyStore keyStore) throws IOException {
        Preconditions.checkNotNull(settings.getAuthSettings());
        try {
            this.settings = settings;

            Class<? extends AzureStorageAuth> ac = (Class<? extends AzureStorageAuth>) Class.forName(settings.getAuthClass());
            auth = ac.getDeclaredConstructor()
                    .newInstance()
                    .withAccount(settings.getAuthAccount())
                    .init(settings.getAuthSettings(), keyStore);

            String endpoint = String.format(Locale.ROOT, settings.getEndpointUrl(), settings.getAuthAccount());
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

}
