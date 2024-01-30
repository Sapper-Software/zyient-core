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

package io.zyient.base.core.keystore;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.ChecksumUtils;
import io.zyient.base.common.utils.CypherUtils;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.keystore.settings.KeyStoreSettings;
import io.zyient.base.core.keystore.settings.ZkKeyStoreSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZkKeyStore extends KeyStore {
    private ZookeeperConnection connection;
    private String passwdHash;
    private DistributedLock updateLock = null;
    private String zkBasePath;

    public ZkKeyStore() {
        super(ZkKeyStoreSettings.class);
    }

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                     @NonNull String password,
                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        Preconditions.checkNotNull(settings());
        try {
            ZkKeyStoreSettings settings = (ZkKeyStoreSettings) settings();
            String pwd = password;
            password = CypherUtils.checkPassword(password, settings.getName());
            withPassword(password);

            HierarchicalConfiguration<ImmutableNode> zkc
                    = config().configurationAt(ConnectionManager.Constants.CONFIG_CONNECTION_LIST);
            connection = new ZookeeperConnection()
                    .withPassword(pwd);
            connection.init(zkc, env);
            connection.connect();
            zkBasePath = new PathUtils.ZkPathBuilder(settings.getZkBasePath())
                    .withPath(settings.getName())
                    .build();
            passwdHash = ChecksumUtils.generateHash(password);
            CuratorFramework client = connection.client();
            if (client.checkExists().forPath(zkBasePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(zkBasePath);
            }
            String passwd = read(KeyStoreSettings.DEFAULT_KEY, password);
            if (passwd == null) {
                save(KeyStoreSettings.DEFAULT_KEY, password, password);
                flush(password);
            } else if (passwd.compareTo(password) != 0) {
                throw new Exception("Invalid password specified....");
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public void save(@NonNull String name,
                     @NonNull String value,
                     @NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        checkLock();
        ZkKeyStoreSettings settings = (ZkKeyStoreSettings) settings();
        updateLock.lock();
        try {
            String path = new PathUtils.ZkPathBuilder(zkBasePath)
                    .withPath(name)
                    .build();
            CuratorFramework client = connection.client();
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
            String encrypted = CypherUtils.encryptAsString(value, password, settings.getIv());
            client.setData().forPath(path, encrypted.getBytes(StandardCharsets.UTF_8));
        } finally {
            updateLock.unlock();
        }
    }

    @Override
    public String read(@NonNull String name,
                       @NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        ZkKeyStoreSettings settings = (ZkKeyStoreSettings) settings();
        CuratorFramework client = connection.client();
        String path = new PathUtils.ZkPathBuilder(zkBasePath)
                .withPath(name)
                .build();
        String value = JSONUtils.read(client, path, String.class);
        if (!Strings.isNullOrEmpty(value)) {
            byte[] data = CypherUtils.decrypt(value, password, settings.getIv());
            return new String(data, StandardCharsets.UTF_8);
        }
        return null;
    }

    @Override
    public void delete(@NonNull String name,
                       @NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        checkLock();
        updateLock.lock();
        try {
            String path = new PathUtils.ZkPathBuilder(zkBasePath)
                    .withPath(name)
                    .build();
            CuratorFramework client = connection.client();
            if (client.checkExists().forPath(path) != null) {
                client.delete().forPath(path);
            }
        } finally {
            updateLock.unlock();
        }
    }

    @Override
    public void delete(@NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        checkLock();
        updateLock.lock();
        try {
            CuratorFramework client = connection.client();
            if (client.checkExists().forPath(zkBasePath) != null) {
                List<String> paths = client.getChildren().forPath(zkBasePath);
                if (paths != null) {
                    for (String p : paths) {
                        String cp = new PathUtils.ZkPathBuilder(zkBasePath)
                                .withPath(p)
                                .build();
                        client.delete().forPath(cp);
                    }
                }
            }
        } finally {
            updateLock.unlock();
        }
    }

    @Override
    public String flush(@NonNull String password) throws Exception {
        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        return zkBasePath;
    }

    private synchronized void checkLock() throws Exception {
        ZkKeyStoreSettings settings = (ZkKeyStoreSettings) settings();
        if (updateLock == null) {
            updateLock = env().createCustomLock(settings.getName(), zkBasePath, connection, 5000L);
        }
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
