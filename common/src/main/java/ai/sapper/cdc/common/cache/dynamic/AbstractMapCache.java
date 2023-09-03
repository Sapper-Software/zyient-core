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

package ai.sapper.cdc.common.cache.dynamic;

import ai.sapper.cdc.common.cache.CacheState;
import ai.sapper.cdc.common.cache.ECacheState;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.entity.IKey;
import ai.sapper.cdc.common.model.entity.IKeyed;
import ai.sapper.cdc.common.threads.ManagedThread;
import ai.sapper.cdc.common.threads.Runner;
import ai.sapper.cdc.common.threads.ThreadManager;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;
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
public abstract class AbstractMapCache<K extends IKey, T extends IKeyed<K>> extends Runner implements Closeable {
    protected final Class<? extends T> entityType;
    @Getter(AccessLevel.NONE)
    protected final ReentrantLock lock = new ReentrantLock();
    protected final CacheState state = new CacheState();
    protected final Class<? extends AbstractDynamicCacheSettings> settingsType;
    protected final ThreadManager manager;

    protected MapCacheLoader<K, T> loader;
    protected ManagedThread loaderThread;
    protected long lastRunTime;
    protected MapCacheSettings settings;

    public AbstractMapCache(@NonNull Class<? extends T> entityType,
                            @NonNull Class<? extends MapCacheSettings> settingsType,
                            @NonNull ThreadManager manager) {
        this.entityType = entityType;
        this.settingsType = settingsType;
        this.manager = manager;
    }

    @SuppressWarnings("unchecked")
    public void configure(@NonNull MapCacheSettings settings,
                          @NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
        try {
            this.settings = settings;
            String lp = ConfigReader.getPathAnnotation(AbstractCacheLoaderSettings.class);
            Preconditions.checkNotNull(lp);
            loader = (MapCacheLoader<K, T>) ReflectionUtils.createInstance(settings.loaderClass)
                    .init(config, lp);
            init(config);
            state.setState(ECacheState.Available);
        } catch (Exception ex) {
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    protected abstract void init(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException;

    public abstract T get(@NonNull K key);

    public abstract Set<K> keySet();

    public abstract Collection<T> values();

    public abstract boolean isEmpty();

    public abstract int size();
}
