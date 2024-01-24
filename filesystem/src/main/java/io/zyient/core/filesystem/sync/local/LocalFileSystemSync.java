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

package io.zyient.core.filesystem.sync.local;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.FileWatcher;
import io.zyient.base.common.utils.FileWatcherFactory;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.impl.local.LocalFileSystem;
import io.zyient.core.filesystem.impl.local.LocalPathInfo;
import io.zyient.core.filesystem.model.DirectoryInode;
import io.zyient.core.filesystem.model.FileInode;
import io.zyient.core.filesystem.model.Inode;
import io.zyient.core.filesystem.sync.EFileSystemSyncState;
import io.zyient.core.filesystem.sync.FileSystemSync;
import io.zyient.core.filesystem.sync.FileSystemSyncSettings;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalFileSystemSync extends FileSystemSync implements FileWatcher.FileWatcherCallback {
    private Map<String, DirectoryInode> domains;
    private List<Pattern> filters;
    private long fullScanTime = 0;
    private final Map<String, LinkedBlockingQueue<EventEntry>> eventQueues = new HashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private final ReentrantLock scanLock = new ReentrantLock();
    private boolean scanRunning = false;

    public LocalFileSystemSync() {
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
            if (((LocalFsSyncSettings) settings).isScanOnStart()) {
                scan();
            } else {
                fullScanTime = System.currentTimeMillis();
            }
            state.setState(EFileSystemSyncState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new IOException(ex);
        } finally {
            try {
                save(state);
            } catch (Exception ex) {
                DefaultLogger.error(ex.getLocalizedMessage());
                DefaultLogger.stacktrace(ex);
            }
        }
    }

    private void scan() throws Exception {
        if (scanRunning) return;
        Scanner scanner = new Scanner((LocalFileSystem) fs, domains, this);
        executor.submit(scanner);
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
        if ((System.currentTimeMillis() - fullScanTime)
                > ((LocalFsSyncSettings) settings).getFullScanInterval().normalized()) {
            scan();
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
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private record EventEntry(WatchEvent.Kind<?> eventType, String path) {
        private EventEntry(@NonNull WatchEvent.Kind<?> eventType,
                           @NonNull String path) {
            this.eventType = eventType;
            this.path = path;
        }
    }

    private record Scanner(LocalFileSystem fs, Map<String, DirectoryInode> domains,
                           LocalFileSystemSync syncer) implements Runnable {


        @Override
        public void run() {
            syncer.scanLock.lock();
            while (!syncer.isRunning()) {
                RunUtils.sleep(10);
            }
            try {
                syncer.scanRunning = true;
                for (String domain : domains.keySet()) {
                    DirectoryInode node = domains.get(domain);
                    File dir = new File(node.getPath());
                    if (dir.exists()) {
                        DefaultLogger.debug(String.format("Scanning domain folder. [path=%s]", dir.getAbsolutePath()));
                    }
                    runScan(domain, dir);
                }
                syncer.fullScanTime = System.currentTimeMillis();
                syncer.state.setTimeSynced(System.currentTimeMillis());
                syncer.save(syncer.state);
                syncer.scanRunning = false;
            } catch (Exception ex) {
                syncer.state.error(ex);
                DefaultLogger.error(ex.getLocalizedMessage());
                DefaultLogger.stacktrace(ex);
            } finally {
                syncer.scanLock.unlock();
            }
        }

        private void runScan(String domain, File dir) throws Exception {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!syncer.doFilter(file.getAbsolutePath()))
                        continue;
                    if (file.isDirectory()) {
                        runScan(domain, file);
                    } else {
                        LocalPathInfo pi = new LocalPathInfo(fs, file.getAbsolutePath(), domain);
                        if (!syncer.checkFileExists(pi)) {
                            Inode node = fs.create(pi);
                            if (node == null) {
                                throw new IOException(
                                        String.format("Failed to create Inode. [domain=%s][path=%s]",
                                                domain, file.getAbsolutePath()));
                            }
                            DefaultLogger.debug(String.format("Created Inode. [domain=%s][path=%s]",
                                    domain, file.getAbsolutePath()));
                        }
                    }
                }
            }
        }
    }
}
