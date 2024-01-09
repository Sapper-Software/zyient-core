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
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.BaseEnv;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AWSKeyStore extends KeyStore {

    private enum KeyState {
        New, Updated, Synced, Deleted
    }

    private static class Key {
        private final String name;
        private String value;
        private KeyState state;

        public Key(String name, String value) {
            this.name = name;
            setValue(value);
        }

        public void setValue(String value) {
            this.value = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
        }

        public String getValue() {
            byte[] v = Base64.getDecoder().decode(value);
            return new String(v, StandardCharsets.UTF_8);
        }
    }

    public static final String CONFIG_REGION = "region";
    public static final String CONFIG_NAME = "name";
    public static final String CONFIG_CACHE_TIMEOUT = "timeout";
    private static final long CACHE_TIMEOUT = 30 * 60 * 1000; // 30mins

    private String name;
    private String passwdHash;
    private SecretsManagerClient client;
    private HierarchicalConfiguration<ImmutableNode> config;
    private String bucket;
    private Map<String, Key> keys;
    private long fetchedTimestamp = 0;
    private long cacheTimeout = CACHE_TIMEOUT;
    private  static  String CONFIG_EXCLUDE_ENV = "excludeEnv";

    private boolean excludeEnv;


    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                     @NonNull String password,
                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            config = configNode.configurationAt(__CONFIG_PATH);
            Preconditions.checkNotNull(config);
            name = config.getString(CONFIG_NAME);
            excludeEnv = config.getBoolean(CONFIG_EXCLUDE_ENV);
            ConfigReader.checkStringValue(name, getClass(), CONFIG_NAME);
            String value = config.getString(CONFIG_REGION);

            ConfigReader.checkStringValue(value, getClass(), CONFIG_REGION);
            Region region = Region.of(value);
            client = SecretsManagerClient.builder()
                    .region(region)
                    .build();
            String s = config.getString(CONFIG_CACHE_TIMEOUT);
            if (!Strings.isNullOrEmpty(s)) {
                cacheTimeout = Long.parseLong(s);
            }
            bucket = name;
            if(!excludeEnv) {
                bucket = String.format("%s/%s", env.name(), name);
            }
            fetch();
            Key key = keys.get(DEFAULT_KEY);
            password = key.getValue();
            if (Strings.isNullOrEmpty(password)) {
                throw new Exception(String.format("Default Secret Key not set. [name=%s]", DEFAULT_KEY));
            }
            withPassword(password);
            passwdHash = ChecksumUtils.generateHash(password);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private synchronized void fetch() throws Exception {
        if (this.keys != null)
            this.keys.clear();
        else
            this.keys = new HashMap<>();
        String json = readInternal(bucket);
        if (!Strings.isNullOrEmpty(json)) {
            Map<String, String> keys = JSONUtils.read(json, Map.class);
            if (!keys.containsKey(DEFAULT_KEY)) {
                throw new Exception(String.format("Invalid Secrets: Default key not found. [name=%s]", bucket));
            }
            for (String name : keys.keySet()) {
                String value = keys.get(name);
                Key key = new Key(name, value);
                key.state = KeyState.Synced;
                this.keys.put(name, key);
            }
            fetchedTimestamp = System.currentTimeMillis();
        } else throw new Exception(String.format("Secrets not found. [name=%s]", bucket));
    }

    @Override
    public void save(@NonNull String name,
                     @NonNull String value,
                     @NonNull String password) throws Exception {
        Preconditions.checkState(client != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        if (DEFAULT_KEY.compareTo(name) == 0) {
            throw new Exception("Default key cannot be updated...");
        }
        try {
            Key key = keys.get(name);
            if (key == null) {
                key = new Key(name, value);
                key.state = KeyState.New;
                keys.put(name, key);
            } else {
                key.setValue(value);
                key.state = KeyState.Updated;
            }
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
            long delta = System.currentTimeMillis() - fetchedTimestamp;
            if (delta > cacheTimeout) {
                fetch();
            }
            Key key = keys.get(name);
            if (key != null) {
                return key.getValue();
            }
            return null;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw ex;
        }
    }

    private String readInternal(@NonNull String name) throws Exception {
        GetSecretValueRequest request = GetSecretValueRequest.builder()
                .secretId(name)
                .build();

        GetSecretValueResponse response = client.getSecretValue(request);
        return response.secretString();
    }

    @Override
    public void delete(@NonNull String name,
                       @NonNull String password) throws Exception {
        Preconditions.checkState(client != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        if (DEFAULT_KEY.compareTo(name) == 0) {
            throw new Exception("Default key cannot be deleted...");
        }
        try {
            Key key = keys.get(name);
            if (key != null) {
                key.state = KeyState.Deleted;
            }
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
    @SuppressWarnings("unchecked")
    public String flush(@NonNull String password) throws Exception {
        Preconditions.checkState(client != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        synchronized (this) {
            try {
                String json = readInternal(bucket);
                Map<String, String> keys = JSONUtils.read(json, Map.class);
                for (String name : this.keys.keySet()) {
                    Key key = this.keys.get(name);
                    if (key.state == KeyState.Synced) continue;
                    if (key.state == KeyState.Deleted) {
                        keys.remove(key.name);
                    } else if (key.state == KeyState.New) {
                        String value = key.getValue();
                        keys.put(name, value);
                    }
                }
                String value = JSONUtils.asString(keys);
                UpdateSecretRequest  secretRequest = UpdateSecretRequest.builder()
                        .secretId(bucket)
                        .description(String.format("[keyStore=%s] Saved secret. [name=%s]", this.name, bucket))
                        .secretString(value)
                        .build();

                UpdateSecretResponse response = client.updateSecret(secretRequest);
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                throw ex;
            }
        }
        fetch();
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
