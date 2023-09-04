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

package io.zyient.base.common.cache.dynamic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.zyient.base.common.cache.ECacheState;
import io.zyient.base.common.cache.CacheException;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.model.entity.IKeyed;
import io.zyient.base.common.threads.ManagedThread;
import io.zyient.base.common.threads.ThreadManager;
import io.zyient.base.common.utils.DefaultLogger;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

@Getter
@Accessors(fluent = true)
public class MultiMapCache<K extends IKey, T extends IKeyed<K>> extends AbstractMultiMapCache<K, T> {
    @Getter(AccessLevel.NONE)
    private Multimap<K, T> cache = null;
    @Getter(AccessLevel.NONE)
    private Multimap<K, T> backupCache = null;
    @Getter(AccessLevel.NONE)
    private Multimap<K, T> cache01 = ArrayListMultimap.create();
    @Getter(AccessLevel.NONE)
    private Multimap<K, T> cache02 = ArrayListMultimap.create();

    public MultiMapCache(@NonNull Class<? extends T> entityType,
                         @NonNull ThreadManager manager) {
        super(entityType, MultiMapCacheSettings.class, manager);
        cache = cache01;
        backupCache = cache02;
    }

    @Override
    protected void init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        try {
            runLoad();
            String name = String.format("%s[%s]", getClass().getSimpleName(), settings.name);
            loaderThread = new ManagedThread(manager, this, name);
            loaderThread.start();
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public Collection<T> get(@NonNull K key) {
        Preconditions.checkState(state.isAvailable());
        if (cache != null && !cache.isEmpty()) return cache.get(key);
        return null;
    }

    @Override
    public Set<K> keySet() {
        Preconditions.checkState(state.isAvailable());
        if (cache != null && !cache.isEmpty()) return cache.keySet();
        return null;
    }

    @Override
    public Collection<T> values() {
        Preconditions.checkState(state.isAvailable());
        if (cache != null && !cache.isEmpty()) return cache.values();
        return null;
    }

    @Override
    public boolean isEmpty() {
        if (state.isAvailable() && cache != null) {
            return cache.isEmpty();
        }
        return true;
    }

    @Override
    public int size() {
        if (state.isAvailable() && cache != null) {
            return cache.size();
        }
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (!state.hasError()) {
            state.setState(ECacheState.Disposed);
        }
        try {
            cache01.clear();
            cache02.clear();
            loader.close();
            loaderThread.join();
        } catch (Exception ex) {
            DefaultLogger.error(getClass().getCanonicalName(), ex);
            throw new IOException(ex);
        }
    }

    @Override
    public void doRun() throws Exception {
        try {
            while (state.isAvailable()) {
                long delta = (System.currentTimeMillis() - lastRunTime);
                if (delta < settings.refreshInterval.normalized()) {
                    try {
                        Thread.sleep(settings.refreshInterval.normalized() - delta);
                    } catch (InterruptedException ie) {
                        // Do nothing...
                    }
                }
                lastRunTime = System.currentTimeMillis();
                if (loader.needsReload()) {
                    runLoad();
                }
            }
        } catch (Exception ex) {
            DefaultLogger.error(getClass().getCanonicalName(), ex);
            throw ex;
        }
    }

    private void runLoad() throws CacheException {
        lock.lock();
        try {
            backupCache.clear();
            Multimap<K, T> data = loader.read(null);
            if (data != null && !data.isEmpty()) {
                backupCache.putAll(data);
                Multimap<K, T> tcache = cache;
                cache = backupCache;
                backupCache = tcache;
                DefaultLogger.info(
                        String.format("Refreshed cache [name=%s]. [#records=%d]", settings.name, data.size()));
            } else {
                DefaultLogger.warn(String.format("No data loaded for cache. [name=%s]", settings.name));
            }
        } finally {
            lock.unlock();
        }
    }
}
