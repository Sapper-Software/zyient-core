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

package io.zyient.core.filesystem.impl;

import io.zyient.base.common.cache.EvictionCallback;
import io.zyient.base.common.cache.LRUCache;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.filesystem.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Getter
@Accessors(fluent = true)
public class RemoteFsCache implements EvictionCallback<String, RemoteFsCache.FsCacheEntry> {

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class FsCacheEntry {
        private String key;
        private File file;
        private FileInode node;
        private long updatedTime;
    }

    private final Map<String, LRUCache<String, FsCacheEntry>> cache = new HashMap<>();
    private final RemoteFileSystem fs;
    private FsCacheSettings settings;

    public RemoteFsCache(@NonNull RemoteFileSystem fs) {
        this.fs = fs;
    }

    public RemoteFsCache init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            if (ConfigReader.checkIfNodeExists(xmlConfig, FsCacheSettings.__CONFIG_PATH)) {
                ConfigReader reader = new ConfigReader(xmlConfig,
                        FsCacheSettings.__CONFIG_PATH,
                        FsCacheSettings.class);
                reader.read();
                settings = (FsCacheSettings) reader.settings();
            } else {
                settings = new FsCacheSettings();
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public RemoteFsCache init(FsCacheSettings settings) throws ConfigurationException {
        if (settings != null) {
            this.settings = settings;
        } else {
            this.settings = new FsCacheSettings();
        }
        return this;
    }

    public File get(@NonNull FileInode inode) throws Exception {
        synchronized (cache) {
            LRUCache<String, FsCacheEntry> cache = getDomainCache(inode.getDomain());
            FsCacheEntry entry = null;
            if (cache.containsKey(inode.getUuid())) {
                Optional<FsCacheEntry> op = cache.get(inode.getUuid());
                if (op.isPresent()) {
                    entry = op.get();
                }
            }
            boolean fetch = false;
            if (entry == null) {
                fetch = true;
            } else if ((System.currentTimeMillis() - entry.updatedTime) > settings.getCacheTimeout()) {
                fetch = true;
            }
            if (fetch) {
                File file = fs.download(inode, settings.getDownloadTimeout());
                if (file == null) return null;
                if (inode.isCompressed()) {
                    File outf = fs.decompress(file);
                    if (!file.delete()) {
                        throw new IOException(String.format("Failed to delete file. [path=%s]", file.getAbsolutePath()));
                    }
                    if (!outf.renameTo(file)) {
                        throw new IOException(String.format("Filed to rename file. [path=%s]", outf.getAbsolutePath()));
                    }
                }
                return put(inode, file);
            }
            return entry.file;
        }
    }

    public File put(@NonNull FileInode inode,
                    @NonNull File path) throws Exception {
        synchronized (cache) {
            LRUCache<String, FsCacheEntry> cache = getDomainCache(inode.getDomain());
            FsCacheEntry entry = null;
            if (cache.containsKey(inode.getUuid())) {
                Optional<FsCacheEntry> op = cache.get(inode.getUuid());
                if (op.isPresent()) {
                    entry = op.get();
                }
            }
            if (entry == null) {
                entry = new FsCacheEntry();
                entry.key = inode.getUuid();
                entry.node = inode;
                cache.put(entry.key, entry);
            }
            entry.file = path;
            entry.updatedTime = System.currentTimeMillis();

            return path;
        }
    }

    private LRUCache<String, FsCacheEntry> getDomainCache(String domain) {
        LRUCache<String, FsCacheEntry> cache = this.cache.get(domain);
        if (cache == null) {
            cache = new LRUCache<String, FsCacheEntry>(settings.getCacheSize())
                    .withEvictionCallback(this);
            this.cache.put(domain, cache);
        }
        return cache;
    }

    @Override
    public void evicted(String key, FsCacheEntry value) {
        if (value.file.exists()) {
            if (!value.file.delete()) {
                DefaultLogger.error(
                        String.format("Failed to delete evicted file. [path=%s]", value.file.getAbsolutePath()));
            }
        }
    }

}
