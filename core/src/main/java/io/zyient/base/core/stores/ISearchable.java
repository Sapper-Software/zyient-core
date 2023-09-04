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

package io.zyient.base.core.stores;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.model.DocumentEntity;
import org.apache.lucene.search.Query;

import javax.annotation.Nonnull;

@SuppressWarnings("rawtypes")
public interface ISearchable {
    <T extends IEntity<?>> BaseSearchResult<T> textSearch(@Nonnull Query query,
                                                       @Nonnull Class<? extends T> type,
                                                       Context context) throws DataStoreException;

    <T extends IEntity<?>> BaseSearchResult<T> textSearch(@Nonnull Query query,
                                                       int batchSize,
                                                       int offset,
                                                       @Nonnull Class<? extends T> type,
                                                       Context context) throws DataStoreException;

    <T extends IEntity<?>> BaseSearchResult<T> textSearch(@Nonnull String query,
                                                       @Nonnull Class<? extends T> type,
                                                       Context context) throws DataStoreException;

    <T extends IEntity<?>> BaseSearchResult<T> textSearch(@Nonnull String query,
                                                       int batchSize,
                                                       int offset,
                                                       @Nonnull Class<? extends T> type,
                                                       Context context) throws DataStoreException;

    <T extends IEntity<?>> BaseSearchResult<T> facetedSearch(Object query,
                                                          @Nonnull Object aggregates,
                                                          @Nonnull Class<? extends T> type,
                                                          Context context) throws DataStoreException;

    void indexDocument(@Nonnull String index, @Nonnull DocumentEntity entity, Context context) throws DataStoreException;

    <C> DocumentEntity indexDocument(@Nonnull String index,
                                     @Nonnull String fileDataStore,
                                     @Nonnull Class<? extends AbstractDataStore<C>> dataStoreType,
                                     @Nonnull IKey key,
                                     @Nonnull Class<? extends IEntity> fileEntityType,
                                     Context context) throws DataStoreException;

    <T extends IEntity<?>> BaseSearchResult<T> searchDocument(@Nonnull String index,
                                                           @Nonnull String query,
                                                           int batchSize,
                                                           int offset,
                                                           Context context) throws DataStoreException;

    <T extends IEntity<?>> BaseSearchResult<T> searchDocument(@Nonnull String index,
                                                           @Nonnull Query query,
                                                           int batchSize,
                                                           int offset,
                                                           @Nonnull Class<? extends T> type,
                                                           Context context) throws DataStoreException;
}
