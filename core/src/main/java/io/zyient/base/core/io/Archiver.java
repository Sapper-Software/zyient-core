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

package io.zyient.base.core.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.connections.Connection;
import io.zyient.base.core.connections.ZookeeperConnection;
import io.zyient.base.core.io.model.*;
import io.zyientj.core.io.model.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Accessors(fluent = true)
public abstract class Archiver implements Closeable {
    protected final Logger LOG = LoggerFactory.getLogger(FileSystem.class);

    private ZookeeperConnection zkConnection;
    protected FSDomainMap domainMap;
    private final Map<String, DirectoryInode> domains = new HashMap<>();
    private final Connection.ConnectionState state = new Connection.ConnectionState();
    private String zkPath;
    private BaseEnv<?> env;
    @Getter(AccessLevel.NONE)
    private File tmpDir;
    private ArchiverSettings settings;

    protected Archiver withZkConnection(@NonNull ZookeeperConnection connection) {
        this.zkConnection = connection;
        return this;
    }

    protected Archiver withZkPath(@NonNull String zkPath) {
        this.zkPath = zkPath;
        return this;
    }

    private void setup() throws Exception {
        if (zkConnection == null) {
            zkConnection = env.connectionManager()
                    .getConnection(settings.getZkConnection(),
                            ZookeeperConnection.class);
            if (zkConnection == null) {
                throw new Exception(String.format("ZooKeeper connection not found. [name=%s]", settings.getZkConnection()));
            }
        }
        if (!zkConnection.isConnected()) {
            zkConnection.connect();
        }
        if (Strings.isNullOrEmpty(zkPath)) {
            if (Strings.isNullOrEmpty(settings.getZkPath())) {
                throw new Exception(
                        String.format("[%s] ZooKeeper path not specified in configuration.", settings.getName()));
            }
            zkPath = new PathUtils.ZkPathBuilder(settings.getZkPath())
                    .withPath(settings.getName())
                    .build();
        } else {
            zkPath = new PathUtils.ZkPathBuilder(zkPath)
                    .withPath("archiver")
                    .withPath(settings.getName())
                    .build();
        }
        Preconditions.checkState(settings.getContainers() != null && !settings.getContainers().isEmpty());
        domainMap = new FSDomainMap(settings.getDefaultContainer(), settings.getContainers());
        CuratorFramework client = zkConnection.client();
        if (client.checkExists().forPath(zkPath) == null) {
            client.create().creatingParentContainersIfNeeded().forPath(zkPath);
            client.setData().forPath(zkPath, null);
        }
        try {
            Collection<Container> domains = domainMap.getDomains();
            try (DistributedLock lock = getRootLock(client)) {
                lock.lock();
                try {
                    for (Container domain : domains) {
                        registerDomain(domain, client);
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            state.error(ex);
            throw new IOException(ex);
        }
        tmpDir = new File(settings.getTempDir());
        if (!tmpDir.exists()) {
            if (!tmpDir.mkdirs()) {
                throw new IOException(
                        String.format("Error create temporary directory. [path=%s]", tmpDir.getAbsolutePath()));
            }
        }
    }


    private void registerDomain(Container container, CuratorFramework client) throws Exception {
        String path = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(container.getDomain())
                .build();
        if (client.checkExists().forPath(path) != null) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                DirectoryInode di = JSONUtils.read(data, DirectoryInode.class);
                domains.put(container.getDomain(), di);
            }
        } else {
            client.create().creatingParentContainersIfNeeded().forPath(path);
            DirectoryInode di = new DirectoryInode(container.getDomain(), "/", "root");
            di.setParent(null);
            di.setPath(container.pathInfo(this).pathConfig());
            di.setUuid(UUID.randomUUID().toString());
            di.setCreateTimestamp(System.currentTimeMillis());
            di.setUpdateTimestamp(System.currentTimeMillis());
            di.setSynced(true);
            di.setZkPath(path);
            di.setAbsolutePath(container.getPath());

            client.setData().forPath(path, JSONUtils.asBytes(di, DirectoryInode.class));
            domains.put(container.getDomain(), di);
        }
    }

    private DistributedLock getRootLock(@NonNull CuratorFramework client) throws Exception {
        String zp = new PathUtils.ZkPathBuilder(zkPath)
                .build();
        if (client.checkExists().forPath(zp) == null) {
            throw new Exception(String.format("Failed to get lock: path not found. [path=%s]", zp));
        }
        return env.createCustomLock(settings.getName(), zp, zkConnection, settings.getLockTimeout());
    }


    public abstract Archiver init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                  String pathPrefix) throws IOException;

    public abstract Archiver init(@NonNull ArchiverSettings settings) throws IOException;

    public abstract ArchivePathInfo archive(@NonNull PathInfo source,
                                            @NonNull ArchivePathInfo target,
                                            @NonNull FileSystem sourceFS) throws IOException;

    public abstract File getFromArchive(@NonNull String domain,
                                        @NonNull String path) throws IOException;
}
