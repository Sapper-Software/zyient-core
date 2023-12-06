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

package io.zyient.base.core.stores.impl.solr;

import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.model.Document;
import io.zyient.base.core.stores.model.DocumentId;
import lombok.NonNull;
import org.apache.lucene.analysis.Analyzer;

import java.util.Map;

public class DocumentQueryBuilder extends EntityQueryBuilder {
    private final String collection;

    public DocumentQueryBuilder(@NonNull Class<? extends Document> entityType,
                                @NonNull Analyzer analyzer,
                                @NonNull String collection) {
        super(entityType, analyzer);
        this.collection = collection;
    }

    public DocumentQueryBuilder(@NonNull Class<?> entityType, String collection) {
        super(entityType);
        this.collection = collection;
    }

    public DocumentQueryBuilder matches(@NonNull String value) throws Exception {
        return (DocumentQueryBuilder) phraseQuery(FIELD_TEXT, value);
    }

    @Override
    public AbstractDataStore.Q build() {
        LuceneQuery q = (LuceneQuery) super.build();
        return q.collection(collection);
    }

    public static AbstractDataStore.Q build(@NonNull Class<? extends Document<?, ?, ?>> entityType,
                                            @NonNull String collection,
                                            @NonNull DocumentId id) throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder(entityType, collection);
        return builder.phraseQuery(SolrConstants.FIELD_SOLR_ID, id.getId()).build();
    }

    public static AbstractDataStore.Q build(@NonNull Class<? extends Document<?, ?, ?>> entityType,
                                            @NonNull String collection,
                                            @NonNull Map<String, String> uri) throws Exception {
        DocumentQueryBuilder builder = new DocumentQueryBuilder(entityType, collection);
        String json = JSONUtils.asString(uri, Map.class);
        return builder.phraseQuery(SolrConstants.FIELD_SOLR_ID, json).build();
    }
}
