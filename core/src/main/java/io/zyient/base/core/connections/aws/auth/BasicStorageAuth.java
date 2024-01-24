/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.connections.aws.auth;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.keystore.KeyStore;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.IOException;

public class BasicStorageAuth implements S3StorageAuth {
    private BasicStorageAuthSettings settings;
    private AwsBasicCredentials credentials;

    @Override
    public S3StorageAuth init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull KeyStore keyStore) throws IOException {
        try {
            ConfigReader reader = new ConfigReader(config,
                    S3StorageAuthSettings.__CONFIG_PATH,
                    BasicStorageAuthSettings.class);
            reader.read();
            return init((S3StorageAuthSettings) reader.settings(), keyStore);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public S3StorageAuth init(@NonNull S3StorageAuthSettings settings,
                              @NonNull KeyStore keyStore) throws IOException {
        Preconditions.checkArgument(settings instanceof BasicStorageAuthSettings);
        try {
            this.settings = (BasicStorageAuthSettings) settings;
            String secretKey = keyStore.read(((BasicStorageAuthSettings) settings).getPassKey());
            if (Strings.isNullOrEmpty(secretKey)) {
                throw new IOException(
                        String.format("Storage Account Key not found. [key=%s]", this.settings.getPassKey()));
            }
            credentials = AwsBasicCredentials.create(this.settings.getAccessKey(), secretKey);
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public S3StorageAuthSettings settings() {
        return settings;
    }

    @Override
    public AwsCredentialsProvider credentials() throws Exception {
        return StaticCredentialsProvider.create(credentials);
    }
}
