/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package ai.sapper.cdc.common.cache.dynamic;

import ai.sapper.cdc.common.cache.CacheException;
import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.common.model.entity.IKey;
import ai.sapper.cdc.common.model.entity.IKeyed;
import com.google.common.collect.Multimap;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;

public interface MultiMapCacheLoader<K extends IKey, T extends IKeyed<K>> extends Closeable {
    boolean needsReload() throws CacheException;

    Multimap<K, T> read(Context context) throws CacheException;

    MultiMapCacheLoader<K, T> init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                   @NonNull String path) throws ConfigurationException;
}
