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

package ai.sapper.cdc.core;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.LockDef;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class DistributedLockBuilder implements Closeable {

    public static class Constants {
        public static final String CONFIG_LOCKS = "locks";
        public static final String CONFIG_ZK_CONN = String.format("%s.connection", CONFIG_LOCKS);
        public static final String CONFIG_LOCK = String.format("%s.lock", CONFIG_LOCKS);
        public static final String CONFIG_LOCK_NAME = "name";
        public static final String CONFIG_LOCK_NODE = "lock-node";
    }

    private ZookeeperConnection connection;
    private BaseEnv<?> env;
    private String zkPath;
    private final Map<String, LockDef> lockDefs = new HashMap<>();
    private final Map<String, DistributedLock> locks = new HashMap<>();

    public DistributedLockBuilder init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                                       @NonNull String module,
                                       @NonNull BaseEnv<?> env) throws Exception {
        this.env = env;
        String zkConn = configNode.getString(Constants.CONFIG_ZK_CONN);
        if (Strings.isNullOrEmpty(zkConn)) {
            throw new Exception(String.format("ZooKeeper connection not defined. [path=%s]", Constants.CONFIG_ZK_CONN));
        }
        connection = env.connectionManager().getConnection(zkConn, ZookeeperConnection.class);
        Preconditions.checkNotNull(connection);
        if (!connection.isConnected()) connection.connect();

        readLocks(configNode);
        readZkLocks();
        return this;
    }

    private String getLockKey(String module, String name) {
        return String.format("%s:%s", module, name);
    }

    private void readLocks(HierarchicalConfiguration<ImmutableNode> configNode) throws Exception {
        List<HierarchicalConfiguration<ImmutableNode>> nodes = configNode.configurationsAt(Constants.CONFIG_LOCK);
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            String name = node.getString(Constants.CONFIG_LOCK_NAME);
            String path = name;
            if (node.containsKey(Constants.CONFIG_LOCK_NODE)) {
                path = node.getString(Constants.CONFIG_LOCK_NODE);
            }
            LockDef def = new LockDef();
            def.setName(name);
            def.setModule(env.module());
            def.setPath(path);

            lockDefs.put(getLockKey(env.module(), name), def);
        }
    }

    private void readZkLocks() throws Exception {
        zkPath = new PathUtils.ZkPathBuilder(env.settings().getRegistryPath())
                .withPath(env.environment())
                .withPath(Constants.CONFIG_LOCKS)
                .build();
        CuratorFramework client = connection.client();
        if (client.checkExists().forPath(zkPath) == null) return;
        List<String> modules = client.getChildren().forPath(zkPath);
        if (modules != null && !modules.isEmpty()) {
            for (String c : modules) {
                String mp = new PathUtils.ZkPathBuilder(zkPath)
                        .withPath(c)
                        .build();
                List<String> locks = client.getChildren().forPath(mp);
                if (locks != null && !locks.isEmpty()) {
                    for (String lc : locks) {
                        String lp = new PathUtils.ZkPathBuilder(mp)
                                .withPath(lc)
                                .build();
                        byte[] data = client.getData().forPath(lp);
                        if (data != null && data.length > 0) {
                            LockDef def = JSONUtils.read(data, LockDef.class);
                            lockDefs.put(getLockKey(def.getModule(), def.getName()), def);
                        }
                    }
                }
            }
        }
    }

    public DistributedLock createLock(@NonNull String path,
                                      @NonNull String module,
                                      @NonNull String name) throws Exception {
        synchronized (this) {
            String key = getLockKey(module, name);
            if (lockDefs().containsKey(key)) {
                LockDef def = lockDefs().get(key);
                if (def == null) {
                    throw new Exception(String.format("No lock definition found: [module=%s][name=%s]", module, name));
                }
                return createLock(def, path, key);
            } else {
                LockDef def = new LockDef();
                def.setName(name);
                def.setModule(module);
                def.setPath(name);
                save(def);

                lockDefs.put(key, def);
                return createLock(def, path, key);
            }
        }
    }

    public DistributedLock createLock(@NonNull String name,
                                      @NonNull String path,
                                      @NonNull ZookeeperConnection connection,
                                      long timeout) throws Exception {
        synchronized (this) {
            DistributedLock lock = locks.get(path);
            if (lock == null) {
                lock = new DistributedLock(name, path, path, this)
                        .withConnection(connection)
                        .withLockTimeout(timeout);
                locks.put(path, lock);
            }
            return lock.incrementReference();
        }
    }

    private DistributedLock createLock(LockDef def, String path, String key) {
        DistributedLock lock = locks.get(key);
        if (lock == null) {
            lock = new DistributedLock(def.getModule(),
                    def.getPath(),
                    path,
                    key,
                    this)
                    .withConnection(connection);
            locks.put(key, lock);
        }
        return lock.incrementReference();
    }

    public boolean removeLock(@NonNull DistributedLock lock) {
        synchronized (this) {
            if (locks.containsKey(lock.key())) {
                lock = locks.get(lock.key());
                if (!lock.hasReference())
                    return (locks.remove(lock.key()) != null);
            }
        }
        return false;
    }

    public void save(@NonNull LockDef def) throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(zkPath));
        CuratorFramework client = connection.client();
        String path = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(def.getModule())
                .withPath(def.getName())
                .build();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        String json = JSONUtils.asString(def, LockDef.class);
        client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
    }


    public void save() throws Exception {
        synchronized (this) {
            if (!lockDefs.isEmpty()) {
                for (String name : lockDefs.keySet()) {
                    save(lockDefs.get(name));
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (!locks.isEmpty()) {
                for (String name : locks.keySet()) {
                    locks.get(name).close();
                }
            }
            locks.clear();
            lockDefs.clear();
        }
    }
}
