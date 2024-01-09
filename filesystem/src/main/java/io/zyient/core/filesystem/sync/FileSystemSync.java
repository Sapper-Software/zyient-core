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

package io.zyient.core.filesystem.sync;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.indexing.FileSystemIndexer;
import io.zyient.core.filesystem.indexing.InodeIndexConstants;
import io.zyient.core.filesystem.model.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import org.apache.curator.framework.CuratorFramework;
import org.apache.lucene.document.Document;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class FileSystemSync implements Closeable, Runnable {
    protected FileSystemSyncState state;
    @Getter
    private final Class<? extends FileSystemSyncSettings> settingsType;

    protected FileSystemSyncSettings settings;
    protected DistributedLock syncLock;
    protected BaseEnv<?> env;
    protected FileSystem fs;
    protected FileSystemIndexer indexer;
    protected ZookeeperConnection connection;
    protected String zkBasePath;

    protected FileSystemSync(@NonNull Class<? extends FileSystemSyncSettings> settingsType) {
        this.settingsType = settingsType;
    }

    public abstract FileSystemSync init(@NonNull FileSystemSyncSettings settings,
                                        @NonNull FileSystem fs,
                                        @NonNull BaseEnv<?> env) throws IOException;

    protected void setup(@NonNull FileSystemSyncSettings settings,
                         @NonNull FileSystem fs,
                         @NonNull BaseEnv<?> env) throws IOException {
        this.env = env;
        this.fs = fs;
        this.settings = settings;
        try {
            indexer = fs.indexer();
            if (indexer == null) {
                throw new IOException(
                        String.format("Indexer not configured. [fs=%s][type=%s]",
                                fs.settings().getName(), fs.getClass().getCanonicalName()));
            }
            connection = env.connectionManager()
                    .getConnection(settings.getZkConnection(), ZookeeperConnection.class);
            if (connection == null) {
                throw new IOException(
                        String.format("ZooKeeper connection not found. [name=%s]",
                                settings.getZkConnection()));
            }
            if (!connection.isConnected()) {
                connection.connect();
            }
            zkBasePath = new PathUtils.ZkPathBuilder(fs.zkPath())
                    .withPath("sync")
                    .build();
            syncLock = env.createLock(getLockPath(), "fs", fs.settings().getName());
            syncLock.lock();
            try {
                state = getState();
                if (state == null) {
                    state = new FileSystemSyncState();
                    state.setTimeSynced(0);
                    state.setFsName(fs.settings().getName());
                    state.setFsZkPath(fs.zkPath());
                    state.setTimeCreated(System.currentTimeMillis());

                    state = save(state);
                }
            } finally {
                syncLock.unlock();
            }
        } catch (Exception ex) {
            if (state != null) {
                state.error(ex);
            }
            throw new IOException(ex);
        }
    }

    protected boolean checkFileExists(@NonNull PathInfo path) throws Exception {
        Preconditions.checkState(isRunning());
        Document doc = indexer.findByFsPath(path.path());
        if (doc != null) {
            String domain = doc.get(InodeIndexConstants.NAME_DOMAIN);
            if (domain.compareTo(path.domain()) == 0)
                return true;
        }
        return (fs.checkPathExists(path));
    }

    protected FileSystemSyncState save(@NonNull FileSystemSyncState state) throws Exception {
        state.setTimeUpdated(System.currentTimeMillis());
        String json = JSONUtils.asString(state);
        CuratorFramework client = connection.client();
        String path = getStatePath();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
        return state;
    }

    private String getLockPath() {
        return new PathUtils.ZkPathBuilder(zkBasePath)
                .withPath("__lock")
                .build();
    }

    private String getStatePath() {
        return new PathUtils.ZkPathBuilder(zkBasePath)
                .withPath("state")
                .build();
    }

    protected FileSystemSyncState getState() throws Exception {
        String path = getStatePath();
        CuratorFramework client = connection.client();
        if (client.checkExists().forPath(path) != null) {
            return JSONUtils.read(client, path, FileSystemSyncState.class);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (isRunning()) {
            state.setState(EFileSystemSyncState.Stopped);
        }
    }

    public boolean isRunning() {
        return (state != null && state.isRunning());
    }

    @Override
    public void run() {
        Preconditions.checkState(state != null
                && state.getState() == EFileSystemSyncState.Initialized);
        try {
            state.setState(EFileSystemSyncState.Running);
            long runTimestamp = state.getTimeSynced();
            long timeout = settings.getFrequency().normalized();
            while (isRunning()) {
                long delta = System.currentTimeMillis() - runTimestamp;
                if (delta > timeout) {
                    syncLock.lock();
                    try {
                        doRun();
                        state.setTimeSynced(System.currentTimeMillis());
                        state = save(state);
                    } finally {
                        syncLock.unlock();
                    }
                } else {
                    RunUtils.sleep(timeout);
                }
            }
            DefaultLogger.warn(
                    String.format("[%s] Terminating file system sync. [state=%s]",
                            getClass().getCanonicalName(), state.getState().name()));
        } catch (Throwable t) {
            state.error(t);
            DefaultLogger.error(
                    String.format("[%s] Terminating file system sync with Error. [state=%s]",
                            getClass().getCanonicalName(), state.getState().name()));
            DefaultLogger.error(t.getLocalizedMessage());
            DefaultLogger.stacktrace(t);
            try {
                save(state);
            } catch (Exception ex) {
                DefaultLogger.error("Failed to save state on error", ex);
                DefaultLogger.stacktrace(ex);
            }
        }
    }

    protected abstract void doRun() throws Exception;
}
