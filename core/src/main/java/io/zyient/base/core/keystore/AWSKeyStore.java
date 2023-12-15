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

package io.zyient.base.core.keystore;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.ChecksumUtils;
import io.zyient.base.common.utils.CypherUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.*;

import java.io.IOException;
import java.util.List;

public class AWSKeyStore extends KeyStore {
    public static final String CONFIG_REGION = "region";
    public static final String CONFIG_NAME = "name";

    private String name;
    private String passwdHash;
    private SecretsManagerClient client;
    private HierarchicalConfiguration<ImmutableNode> config;

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                     @NonNull String password,
                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            config = configNode.configurationAt(__CONFIG_PATH);
            Preconditions.checkNotNull(config);
            name = config.getString(CONFIG_NAME);
            ConfigReader.checkStringValue(name, getClass(), CONFIG_NAME);
            String value = config.getString(CONFIG_REGION);
            ConfigReader.checkStringValue(value, getClass(), CONFIG_REGION);
            Region region = Region.of(value);
            client = SecretsManagerClient.builder()
                    .region(region)
                    .build();
            password = CypherUtils.checkPassword(password, name);
            withPassword(password);
            passwdHash = ChecksumUtils.generateHash(password);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void save(@NonNull String name,
                     @NonNull String value,
                     @NonNull String password) throws Exception {
        Preconditions.checkState(client != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        try {
            CreateSecretRequest secretRequest = CreateSecretRequest.builder()
                    .name(name)
                    .description(String.format("[keyStore=%s] Saved secret. [name=%s]", this.name, name))
                    .secretString(value)
                    .build();

            CreateSecretResponse response = client.createSecret(secretRequest);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw ex;
        }
    }

    @Override
    public String read(@NonNull String name, @NonNull String password) throws Exception {
        Preconditions.checkState(client != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        try {
            GetSecretValueRequest request = GetSecretValueRequest.builder()
                    .secretId(name)
                    .build();

            GetSecretValueResponse response = client.getSecretValue(request);
            return response.secretString();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw ex;
        }
    }

    @Override
    public void delete(@NonNull String name,
                       @NonNull String password) throws Exception {
        Preconditions.checkState(client != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        try {
            DeleteSecretRequest secretRequest = DeleteSecretRequest.builder()
                    .secretId(name)
                    .build();

            DeleteSecretResponse response = client.deleteSecret(secretRequest);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw ex;
        }
    }

    @Override
    public void delete(@NonNull String password) throws Exception {
        Preconditions.checkState(client != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        try {
            ListSecretsResponse response = client.listSecrets();
            List<SecretListEntry> secrets = response.secretList();
            for (SecretListEntry secret : secrets) {
                delete(secret.name(), password);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw ex;
        }
    }

    @Override
    public String flush(@NonNull String password) throws Exception {
        return name;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
