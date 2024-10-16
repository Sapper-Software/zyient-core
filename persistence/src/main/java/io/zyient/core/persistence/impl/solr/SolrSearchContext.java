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

package io.zyient.core.persistence.impl.solr;

import io.zyient.core.persistence.SearchContext;
import lombok.NonNull;
import org.apache.lucene.analysis.Analyzer;

public class SolrSearchContext extends SearchContext {
    public static final String KEY_QUERY_ANALYZER = "search.query.analyzer";



    public SolrSearchContext analyzer(@NonNull Analyzer analyzer) {
        return (SolrSearchContext) put(KEY_QUERY_ANALYZER, analyzer);
    }

    public Analyzer analyzer() {
        return (Analyzer) get(KEY_QUERY_ANALYZER);
    }
}
