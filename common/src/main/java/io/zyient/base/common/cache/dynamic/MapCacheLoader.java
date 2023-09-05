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

import io.zyient.base.common.cache.CacheException;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.model.entity.IKeyed;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.util.Collection;

public interface MapCacheLoader<K extends IKey, T extends IKeyed<K>> extends Closeable {
    boolean needsReload() throws CacheException;

    Collection<T> read(Context context) throws CacheException;

    MapCacheLoader<K, T> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              @NonNull String path) throws ConfigurationException;
}
