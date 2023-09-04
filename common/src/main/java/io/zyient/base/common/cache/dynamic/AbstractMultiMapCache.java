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
import io.zyient.base.common.cache.CacheState;
import io.zyient.base.common.cache.ECacheState;
import io.zyient.base.common.threads.ManagedThread;
import io.zyient.base.common.threads.Runner;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.model.entity.IKeyed;
import io.zyient.base.common.threads.ThreadManager;
import io.zyient.base.common.utils.ReflectionUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Accessors(fluent = true)
public abstract class AbstractMultiMapCache<K extends IKey, T extends IKeyed<K>> extends Runner implements Closeable {
    protected final Class<? extends T> entityType;
    protected final ReentrantLock lock = new ReentrantLock();
    protected final Class<? extends MultiMapCacheSettings> settingsType;
    protected final CacheState state = new CacheState();
    protected final ThreadManager manager;

    protected MultiMapCacheSettings settings;
    @Getter(AccessLevel.NONE)
    protected ManagedThread loaderThread;
    @Getter(AccessLevel.NONE)
    protected long lastRunTime;
    protected MultiMapCacheLoader<K, T> loader;

    public AbstractMultiMapCache(@NonNull Class<? extends T> entityType,
                                 @NonNull Class<? extends MultiMapCacheSettings> settingsType,
                                 @NonNull ThreadManager manager) {
        this.entityType = entityType;
        this.settingsType = settingsType;
        this.manager = manager;
    }

    @SuppressWarnings("unchecked")
    public void configure(@NonNull MultiMapCacheSettings settings,
                          @NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        try {
            this.settings = settings;
            String lp = ConfigReader.getPathAnnotation(AbstractCacheLoaderSettings.class);
            Preconditions.checkNotNull(lp);
            loader = (MultiMapCacheLoader<K, T>) ReflectionUtils.createInstance(settings.loaderClass)
                    .init(config, lp);
            init(config);
            state.setState(ECacheState.Available);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }


    protected abstract void init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException;

    public abstract Collection<T> get(@NonNull K key);

    public abstract Set<K> keySet();

    public abstract Collection<T> values();

    public abstract boolean isEmpty();

    public abstract int size();
}
