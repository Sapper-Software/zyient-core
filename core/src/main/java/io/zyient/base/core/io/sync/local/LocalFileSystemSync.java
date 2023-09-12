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

package io.zyient.base.core.io.sync.local;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.FileWatcher;
import io.zyient.base.common.utils.FileWatcherFactory;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.io.impl.local.LocalFileSystem;
import io.zyient.base.core.io.impl.local.LocalPathInfo;
import io.zyient.base.core.io.model.DirectoryInode;
import io.zyient.base.core.io.model.FileInode;
import io.zyient.base.core.io.model.Inode;
import io.zyient.base.core.io.sync.FileSystemSync;
import io.zyient.base.core.io.sync.FileSystemSyncSettings;
import lombok.NonNull;

import java.io.IOException;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalFileSystemSync extends FileSystemSync implements FileWatcher.FileWatcherCallback {
    private Map<String, DirectoryInode> domains;
    private List<Pattern> filters;
    private long fullScanTime = 0;
    private final Map<String, LinkedBlockingQueue<EventEntry>> eventQueues = new HashMap<>();

    protected LocalFileSystemSync() {
        super(LocalFsSyncSettings.class);
    }

    @Override
    public FileSystemSync init(@NonNull FileSystemSyncSettings settings,
                               @NonNull FileSystem fs,
                               @NonNull BaseEnv<?> env) throws IOException {
        Preconditions.checkArgument(settings instanceof LocalFsSyncSettings);
        Preconditions.checkArgument(fs instanceof LocalFileSystem);
        try {
            super.setup(settings, fs, env);
            LocalFileSystem lfs = (LocalFileSystem) fs;
            domains = lfs.domains();
            List<String> ff = ((LocalFsSyncSettings) settings).getFilters();
            if (ff != null && !ff.isEmpty()) {
                filters = new ArrayList<>(ff.size());
                for (String f : ff) {
                    Pattern p = Pattern.compile(f);
                    filters.add(p);
                }
            }
            for (String domain : domains.keySet()) {
                DirectoryInode di = domains.get(domain);
                eventQueues.put(domain, new LinkedBlockingQueue<>());
                FileWatcherFactory.create(domain, di.getFsPath(), null, this);
            }
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new IOException(ex);
        }
    }

    @Override
    protected void doRun() throws Exception {
        synchronized (eventQueues) {
            for (String domain : eventQueues.keySet()) {
                LinkedBlockingQueue<EventEntry> queue = eventQueues.get(domain);
                while (true) {
                    EventEntry e = queue.poll(50, TimeUnit.MILLISECONDS);
                    if (e == null) {
                        break;
                    }
                    if (!doFilter(e.path)) {
                        continue;
                    }
                    LocalPathInfo pi = new LocalPathInfo(fs, e.path, domain);
                    if (isKind(e.eventType, StandardWatchEventKinds.ENTRY_CREATE)) {
                        if (!checkFileExists(pi)) {
                            Inode node = fs.create(pi);
                            if (node == null) {
                                throw new IOException(
                                        String.format("Failed to create Inode. [domain=%s][path=%s]",
                                                domain, e.path));
                            }
                            DefaultLogger.debug(String.format("Created Inode. [domain=%s][path=%s]",
                                    domain, e.path));
                        }
                    } else if (isKind(e.eventType, StandardWatchEventKinds.ENTRY_MODIFY)) {
                        if (!checkFileExists(pi)) {
                            Inode node = fs.create(pi);
                            if (node == null) {
                                throw new IOException(
                                        String.format("Failed to create Inode. [domain=%s][path=%s]",
                                                domain, e.path));
                            }
                            DefaultLogger.debug(String.format("Created Inode. [domain=%s][path=%s]",
                                    domain, e.path));
                        } else {
                            Inode node = fs.getInode(domain, e.path);
                            if (node == null) {
                                throw new IOException(
                                        String.format("Failed to get Inode. [domain=%s][path=%s]",
                                                domain, e.path));
                            }
                            if (node instanceof FileInode) {
                                ((FileInode) node).setDataSize(pi.dataSize());
                                node.setUpdateTimestamp(System.currentTimeMillis());
                                fs.updateInodeWithLock(node);
                            }
                        }
                    } else if (isKind(e.eventType, StandardWatchEventKinds.ENTRY_DELETE)) {
                        if (checkFileExists(pi)) {
                            Inode node = fs.getInode(domain, e.path);
                            if (node != null) {
                                fs.delete(pi, true);
                                DefaultLogger.debug(String.format("Deleted Inode. [domain=%s][path=%s]",
                                        domain, e.path));
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean doFilter(String path) {
        if (filters != null && !filters.isEmpty()) {
            for (Pattern filter : filters) {
                Matcher m = filter.matcher(path);
                if (m.matches()) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    private boolean isKind(WatchEvent.Kind<?> source, WatchEvent.Kind<?> target) {
        return (source.name().compareTo(target.name()) == 0);
    }

    @Override
    public void handle(@NonNull String watcher,
                       @NonNull String path,
                       WatchEvent.Kind<?> eventKind) throws IOException {
        LinkedBlockingQueue<EventEntry> queue = eventQueues.get(watcher);
        if (queue == null) {
            DefaultLogger.error(String.format("Event Queue not created. [domain=%s]", watcher));
        } else
            queue.add(new EventEntry(eventKind, path));
    }

    @Override
    public void close() throws IOException {
        super.close();
        FileWatcherFactory.shutdown();
    }

    private static class EventEntry {
        private final WatchEvent.Kind<?> eventType;
        private final String path;

        public EventEntry(@NonNull WatchEvent.Kind<?> eventType,
                          @NonNull String path) {
            this.eventType = eventType;
            this.path = path;
        }
    }
}
