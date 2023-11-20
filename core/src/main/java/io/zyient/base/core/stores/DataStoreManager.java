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

package io.zyient.base.core.stores;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.cache.MapThreadCache;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.processing.ProcessorState;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class DataStoreManager {
    private static class ZkSequenceBlock {
        private long next;
        private long endSequence;
    }

    private final ProcessorState state = new ProcessorState();
    private final Map<Class<? extends IEntity<?>>, Map<Class<? extends AbstractDataStore<?>>, AbstractDataStoreSettings>> entityIndex = new HashMap<>();
    private final Map<String, AbstractDataStoreSettings> dataStoreConfigs = new HashMap<>();
    private final Map<String, ZkSequenceBlock> sequences = new HashMap<>();
    private final MapThreadCache<String, AbstractDataStore<?>> openedStores = new MapThreadCache<>();
    private final Map<String, AbstractDataStore<?>> safeStores = new HashMap<>();

    private BaseEnv<?> env;
    private ConnectionManager connectionManager;
    private DataStoreManagerSettings settings;
    private DistributedLock updateLock;
    private ZookeeperConnection zkConnection;
    private String zkPath;

    public boolean isTypeSupported(@Nonnull Class<?> type) {
        if (ReflectionUtils.implementsInterface(IEntity.class, type)) {
            return entityIndex.containsKey(type);
        }
        return false;
    }

    public <T extends AbstractConnection<?>> T getConnection(@Nonnull String name,
                                                             Class<? extends T> type) throws DataStoreException {
        try {
            state.check(ProcessorState.EProcessorState.Running);
            return connectionManager.getConnection(name, type);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <T> AbstractConnection<T> getConnection(@Nonnull Class<? extends IEntity<?>> type) throws DataStoreException {
        return getConnection(type, false);
    }

    @SuppressWarnings({"unchecked"})
    public <T> AbstractConnection<T> getConnection(@Nonnull Class<? extends IEntity<?>> type,
                                                   boolean checkSuperTypes) throws DataStoreException {
        try {
            state.check(ProcessorState.EProcessorState.Running);
            Class<? extends IEntity<?>> ct = type;
            while (true) {
                if (entityIndex.containsKey(ct)) {
                    return (AbstractConnection<T>) entityIndex.get(ct);
                }
                if (checkSuperTypes) {
                    Class<?> t = ct.getSuperclass();
                    if (ReflectionUtils.implementsInterface(IEntity.class, t)) {
                        ct = (Class<? extends IEntity<?>>) t;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <S extends AbstractDataStore<?>> S getDataStore(@Nonnull String name,
                                                           @Nonnull Class<? extends S> storeType) throws DataStoreException {
        return getDataStore(name, storeType, true);
    }

    public <S extends AbstractDataStore<?>> S getDataStore(@Nonnull String name,
                                                           @Nonnull Class<? extends S> storeType,
                                                           boolean add) throws DataStoreException {
        try {
            AbstractDataStoreSettings config = dataStoreConfigs.get(name);
            if (config == null) {
                throw new DataStoreException(
                        String.format("No configuration found for data store type. [type=%s]", storeType.getCanonicalName()));
            }
            if (!config.getDataStoreClass().equals(storeType)) {
                throw new DataStoreException(
                        String.format("Invalid Data Store class. [store=%s][expected=%s][configured=%s]",
                                name, storeType.getCanonicalName(), config.getDataStoreClass().getCanonicalName()));
            }
            return getDataStore(config, storeType, add);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public <T, E extends IEntity> AbstractDataStore<T> getDataStore(@Nonnull Class<? extends AbstractDataStore<T>> storeType,
                                                                    Class<? extends E> type) throws DataStoreException {
        return getDataStore(storeType, type, true);
    }

    @SuppressWarnings({"rawtypes"})
    public <S extends AbstractDataStore<?>, E extends IEntity> S getDataStore(@Nonnull Class<? extends S> storeType,
                                                                              Class<? extends E> type,
                                                                              boolean add) throws DataStoreException {
        Map<Class<? extends AbstractDataStore<?>>, AbstractDataStoreSettings> configs = entityIndex.get(type);
        if (configs == null) {
            throw new DataStoreException(String.format("No data store found for entity type. [type=%s]", type.getCanonicalName()));
        }
        AbstractDataStoreSettings config = configs.get(storeType);
        if (config == null) {
            throw new DataStoreException(String.format("No data store found. [type=%s][store type=%s]", type.getCanonicalName(), storeType.getCanonicalName()));
        }

        try {
            return getDataStore(config, storeType, add);
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private <S extends AbstractDataStore<?>> S getDataStore(AbstractDataStoreSettings config,
                                                            Class<? extends S> storeType,
                                                            boolean add) throws DataStoreException {
        Preconditions.checkNotNull(env);
        if (safeStores.containsKey(config.getName())) {
            return (S) safeStores.get(config.getName());
        }
        Map<String, AbstractDataStore<?>> stores = null;
        if (openedStores.containsThread()) {
            stores = openedStores.get();
            if (stores.containsKey(config.getName())) {
                return (S) stores.get(config.getName());
            }
        } else if (!add) {
            return null;
        }

        try {
            S store = ReflectionUtils.createInstance(storeType);
            store.configure(this, config, env);
            if (store.isThreadSafe()) {
                safeStores.put(store.name(), store);
            } else
                openedStores.put(store.name(), store);

            return store;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public void commit() throws DataStoreException {
        try {
            if (openedStores.containsThread()) {
                Map<String, AbstractDataStore<?>> stores = openedStores.get();
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store.auditLogger() != null) {
                        store.auditLogger().flush();
                    }
                }
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store instanceof TransactionDataStore) {
                        if (((TransactionDataStore) store).isInTransaction()) {
                            ((TransactionDataStore) store).commit();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public void rollback() throws DataStoreException {
        try {
            if (openedStores.containsThread()) {
                Map<String, AbstractDataStore<?>> stores = openedStores.get();
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store.auditLogger() != null) {
                        store.auditLogger().discard();
                    }
                }
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store instanceof TransactionDataStore) {
                        if (((TransactionDataStore) store).isInTransaction()) {
                            ((TransactionDataStore) store).rollback();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public void closeStores() throws DataStoreException {
        try {
            if (openedStores.containsThread()) {
                Map<String, AbstractDataStore<?>> stores = openedStores.get();
                List<AbstractDataStore> storeList = new ArrayList<>();
                for (String name : stores.keySet()) {
                    AbstractDataStore<?> store = stores.get(name);
                    if (store.auditLogger() != null) store.auditLogger().discard();
                    if (store instanceof TransactionDataStore) {
                        if (((TransactionDataStore) store).isInTransaction()) {
                            DefaultLogger.error(
                                    String.format("Store has pending transactions, rolling back. [name=%s][thread id=%d]",
                                            store.name(), Thread.currentThread().getId()));
                            ((TransactionDataStore) store).rollback();
                        }
                    }
                    storeList.add(store);
                }
                for (AbstractDataStore store : storeList) {
                    try {
                        store.close();
                    } catch (IOException e) {
                        DefaultLogger.error(e.getLocalizedMessage());
                    }
                }
                openedStores.clear();
                storeList.clear();
            }
        } catch (Throwable t) {
            throw new DataStoreException(t);
        }
    }

    public void close(@Nonnull AbstractDataStore dataStore) throws DataStoreException {
        try {
            if (openedStores.containsThread()) {
                Map<String, AbstractDataStore<?>> stores = openedStores.get();
                if (stores.containsKey(dataStore.name())) {
                    if (dataStore.auditLogger() != null) {
                        dataStore.auditLogger().discard();
                    }
                    if (dataStore instanceof TransactionDataStore) {
                        TransactionDataStore ts = (TransactionDataStore) dataStore;
                        if (ts.isInTransaction()) {
                            DefaultLogger.error(
                                    String.format("Data Store has un-committed transaction. [name=%s][thread=%d]",
                                            dataStore.name(), Thread.currentThread().getId()));
                            ts.rollback();
                        }
                    }
                    openedStores.remove(dataStore.name());
                }
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                     @Nonnull BaseEnv<?> env,
                     String path) throws ConfigurationException {
        this.env = env;
        this.connectionManager = env.connectionManager();
        try {
            String lp = new PathUtils.ZkPathBuilder("stores")
                    .withPath(getClass().getSimpleName())
                    .build();
            updateLock = env.createLock(lp);
            updateLock.lock();
            try {
                if (!ConfigReader.checkIfNodeExists(xmlConfig, DataStoreManagerSettings.CONFIG_NODE_DATA_STORES)) {
                    state.setState(ProcessorState.EProcessorState.Running);
                    return;
                }
                HierarchicalConfiguration<ImmutableNode> config
                        = xmlConfig.configurationAt(DataStoreManagerSettings.CONFIG_NODE_DATA_STORES);
                ConfigReader reader = new ConfigReader(config, null, DataStoreManagerSettings.class);
                reader.read();
                settings = (DataStoreManagerSettings) reader.settings();
                if (Strings.isNullOrEmpty(path)) {
                    ConfigPath cp = AbstractDataStoreSettings.class.getAnnotation(ConfigPath.class);
                    path = cp.path();
                }
                if (ConfigReader.checkIfNodeExists(config, path)) {
                    List<HierarchicalConfiguration<ImmutableNode>> dsnodes = config.configurationsAt(path);
                    for (HierarchicalConfiguration<ImmutableNode> node : dsnodes) {
                        readDataStoreConfig(node);
                    }
                }
                if (!Strings.isNullOrEmpty(settings.getZkConnection())) {
                    zkConnection = connectionManager
                            .getConnection(settings.getZkConnection(), ZookeeperConnection.class);
                    if (zkConnection == null) {
                        throw new Exception(String.format("ZooKeeper connection not found. [name=%s]",
                                settings.getZkConnection()));
                    }
                    if (!zkConnection.isConnected()) zkConnection.connect();
                    String zp = settings.getZkPath();
                    if (Strings.isNullOrEmpty(zp)) {
                        zp = env.settings().getRegistryPath();
                    }
                    zkPath = new PathUtils.ZkPathBuilder(zp)
                            .withPath(env.environment())
                            .build();
                    readFromZk(false);
                }
            } finally {
                updateLock.unlock();
            }
            state.setState(ProcessorState.EProcessorState.Running);
            if (settings.isAutoSave()) {
                save();
            }
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    public void reLoadConfigurations() throws DataStoreException {
        updateLock.lock();
        try {
            readFromZk(true);
        } finally {
            updateLock.unlock();
        }
    }

    private void readFromZk(boolean reload) throws DataStoreException {
        Preconditions.checkNotNull(zkConnection);
        Preconditions.checkState(!Strings.isNullOrEmpty(zkPath));
        try {
            CuratorFramework client = zkConnection.client();
            String dspath = new PathUtils.ZkPathBuilder(zkPath)
                    .withPath(DataStoreManagerSettings.CONFIG_NODE_DATA_STORES)
                    .build();
            if (client.checkExists().forPath(dspath) != null) {
                List<String> types = client.getChildren().forPath(dspath);
                if (types != null && !types.isEmpty()) {
                    for (String type : types) {
                        String tp = new PathUtils.ZkPathBuilder(dspath)
                                .withPath(type)
                                .build();
                        List<String> names = client.getChildren().forPath(tp);
                        if (names != null && !names.isEmpty()) {
                            for (String name : names) {
                                if (dataStoreConfigs.containsKey(name) && settings.isOverride()) {
                                    continue;
                                }
                                String cp = new PathUtils.ZkPathBuilder(tp)
                                        .withPath(name)
                                        .build();
                                readDataStoreConfig(client, cp, name);
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    public void save() throws DataStoreException {
        if (zkConnection == null) {
            return;
        }
        try {
            state.check(ProcessorState.EProcessorState.Running);
            updateLock.lock();
            try {
                if (!dataStoreConfigs.isEmpty()) {
                    for (String name : dataStoreConfigs.keySet()) {
                        AbstractDataStoreSettings config = dataStoreConfigs.get(name);
                        save(config);
                    }
                }
            } finally {
                updateLock.unlock();
            }
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }

    private void save(AbstractDataStoreSettings settings) throws Exception {
        CuratorFramework client = zkConnection.client();
        String dspath = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(DataStoreManagerSettings.CONFIG_NODE_DATA_STORES)
                .withPath(settings.getType().name())
                .withPath(settings.getName())
                .build();
        if (client.checkExists().forPath(dspath) == null) {
            client.create().creatingParentsIfNeeded().forPath(dspath);
        }
        JSONUtils.write(client, dspath, settings);
    }

    private void readDataStoreConfig(CuratorFramework client,
                                     String path,
                                     String name) throws Exception {
        byte[] data = client.getData().forPath(path);
        if (data == null || data.length == 0) {
            throw new Exception(String.format("DataStore configuration not found. [path=%s]", path));
        }
        AbstractDataStoreSettings settings = JSONUtils.read(data, AbstractDataStoreSettings.class);
        settings.setSource(EConfigSource.Database);
        addDataStoreConfig(settings);
    }

    @SuppressWarnings("unchecked")
    public void addDataStoreConfig(AbstractDataStoreSettings config) throws ConfigurationException {
        try {
            dataStoreConfigs.put(
                    config.getName(), config);
            AbstractConnection<?> connection = connectionManager
                    .getConnection(config.getConnectionName(), config.getConnectionType());
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("No connection found. [store=%s][connection=%s]",
                                config.getName(), config.getConnectionName()));
            }
            if (connection.getSupportedTypes() != null) {
                for (Class<?> t : connection.getSupportedTypes()) {
                    if (ReflectionUtils.implementsInterface(IEntity.class, t)) {
                        Map<Class<? extends AbstractDataStore<?>>, AbstractDataStoreSettings> ec = entityIndex.get(t);
                        if (ec == null) {
                            ec = new HashMap<>();
                            entityIndex.put((Class<? extends IEntity<?>>) t, ec);
                        }
                        ec.put(config.getDataStoreClass(), config);
                    }
                }
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private void readDataStoreConfig(HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            Class<?> type = ConfigReader.readAsClass(xmlConfig, AbstractDataStoreSettings.CONFIG_SETTING_TYPE);
            if (!ReflectionUtils.isSuperType(AbstractDataStoreSettings.class, type)) {
                throw new ConfigurationException(
                        String.format("Invalid settings type. [type=%s]", type.getCanonicalName()));
            }
            ConfigReader reader = new ConfigReader(xmlConfig, null, (Class<? extends Settings>) type);
            reader.read();
            AbstractDataStoreSettings settings = (AbstractDataStoreSettings) reader.settings();
            settings.setSource(EConfigSource.File);
            addDataStoreConfig(settings);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public long nextSequence(String name, String sequenceName) throws DataStoreException {
        synchronized (sequences) {
            String key = String.format("%s::%s", name, sequenceName);
            if (sequences.containsKey(key)) {
                ZkSequenceBlock sq = sequences.get(key);
                if (sq.next < sq.endSequence) {
                    return sq.next++;
                }
            }
            if (zkConnection != null) {
                AbstractDataStoreSettings settings = dataStoreConfigs.get(name);
                if (settings == null) {
                    throw new DataStoreException(String.format("DataStore not found. [name=%s]", name));
                }
                updateLock.lock();
                try {
                    ConfigPath cp = ZkSequence.class.getAnnotation(ConfigPath.class);

                    CuratorFramework client = zkConnection.client();
                    String dspath = new PathUtils.ZkPathBuilder(zkPath)
                            .withPath(DataStoreManagerSettings.CONFIG_NODE_DATA_STORES)
                            .withPath(settings.getType().name())
                            .withPath(settings.getName())
                            .withPath(cp.path())
                            .withPath(sequenceName)
                            .build();
                    ZkSequence sequence = null;
                    if (client.checkExists().forPath(dspath) == null) {
                        client.create().creatingParentsIfNeeded().forPath(dspath);
                        sequence = new ZkSequence();
                    } else {
                        sequence = JSONUtils.read(client, dspath, ZkSequence.class);
                        if (sequence == null) {
                            sequence = new ZkSequence();
                        }
                    }
                    long next = sequence.getNext();
                    sequence.setNext(sequence.getNext() + settings.getSequenceBlockSize());
                    sequence.setTimeUpdated(System.currentTimeMillis());
                    JSONUtils.write(client, dspath, sequence);

                    ZkSequenceBlock block = new ZkSequenceBlock();
                    block.next = next;
                    block.endSequence = sequence.getNext();
                    sequences.put(key, block);

                    return block.next++;
                } catch (Exception ex) {
                    throw new DataStoreException(ex);
                } finally {
                    updateLock.unlock();
                }
            }
        }
        throw new DataStoreException("ZK Sequence not supported...");
    }
}
