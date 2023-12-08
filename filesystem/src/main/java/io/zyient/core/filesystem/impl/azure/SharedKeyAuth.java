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

package io.zyient.core.filesystem.impl.azure;

import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.keystore.KeyStore;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

public class SharedKeyAuth implements AzureStorageAuth {

    private String account;
    private StorageSharedKeyCredential credential;
    private SharedKeyAuthSettings settings;

    @Override
    public AzureStorageAuth withAccount(@NonNull String account) {
        this.account = account;
        return this;
    }

    @Override
    public AzureStorageAuth init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                 @NonNull KeyStore keyStore) throws IOException {
        Preconditions.checkState(!Strings.isNullOrEmpty(account));
        try {
            ConfigReader reader = new ConfigReader(config,
                    AzureStorageAuthSettings.__CONFIG_PATH,
                    SharedKeyAuthSettings.class);
            reader.read();
            settings = (SharedKeyAuthSettings) reader.settings();
            String accountKey = keyStore.read(settings.getAuthKey());
            if (Strings.isNullOrEmpty(accountKey)) {
                throw new IOException(String.format("Storage Account Key not found. [key=%s]", settings.getAuthKey()));
            }
            credential = new StorageSharedKeyCredential(account, accountKey);
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public AzureStorageAuth init(@NonNull AzureStorageAuthSettings settings,
                                 @NonNull KeyStore keyStore) throws IOException {
        Preconditions.checkState(!Strings.isNullOrEmpty(account));
        Preconditions.checkArgument(settings instanceof SharedKeyAuthSettings);
        try {
            this.settings = (SharedKeyAuthSettings) settings;
            String accountKey = keyStore.read(this.settings.getAuthKey());
            if (Strings.isNullOrEmpty(accountKey)) {
                throw new IOException(
                        String.format("Storage Account Key not found. [key=%s]", this.settings.getAuthKey()));
            }
            credential = new StorageSharedKeyCredential(account, accountKey);
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public AzureStorageAuthSettings settings() {
        return settings;
    }

    @Override
    public BlobServiceClientBuilder credentials(@NonNull BlobServiceClientBuilder builder) throws Exception {
        Preconditions.checkNotNull(credential);
        return builder.credential(credential);
    }
}
