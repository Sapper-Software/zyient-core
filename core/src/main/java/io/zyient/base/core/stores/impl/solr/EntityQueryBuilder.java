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

import io.zyient.base.common.model.PropertyModel;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.base.core.stores.AbstractDataStore;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;

import javax.annotation.Nonnull;
import java.lang.reflect.Modifier;

@Getter
@Accessors(fluent = true)
public class EntityQueryBuilder<K extends IKey, E extends IEntity<K>> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class LuceneQuery extends AbstractDataStore.Q {
        private Query query;
    }

    private final Class<? extends E> entityType;
    private final Analyzer analyzer;

    private final QueryBuilder builder;
    private Query query;

    public EntityQueryBuilder(@Nonnull Class<? extends E> entityType,
                              @Nonnull Analyzer analyzer) {
        this.analyzer = analyzer;
        this.entityType = entityType;
        builder = new QueryBuilder(analyzer);
    }


    private void checkProperty(String field) throws Exception {
        PropertyModel pm = ReflectionHelper.findProperty(entityType, field);
        if (pm == null) {
            throw new Exception(String.format("Property not found. [type=%s][field=%s]",
                    entityType.getCanonicalName(), field));
        }
        if (pm.field() == null
                || Modifier.isPrivate(pm.field().getModifiers())
                || Modifier.isProtected(pm.field().getModifiers())) {
            if (pm.getter() == null) {
                throw new Exception(String.format("Property not accessible. [type=%s][field=%s]",
                        entityType.getCanonicalName(), field));
            }
        }
    }

    /**
     * Creates a boolean query from the query text.
     *
     * <p>This is equivalent to {@code createBooleanQuery(field, queryText, Occur.SHOULD)}
     *
     * @param field     field name
     * @param queryText text to be passed to the analyzer
     * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis of {@code queryText}
     */
    public EntityQueryBuilder<K, E> createBooleanQuery(String field, String queryText) throws Exception {
        checkProperty(field);
        query = builder.createBooleanQuery(field, queryText);
        return this;
    }

    /**
     * Creates a boolean query from the query text.
     *
     * @param field     field name
     * @param queryText text to be passed to the analyzer
     * @param operator  operator used for clauses between analyzer tokens.
     * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis of {@code queryText}
     */
    public EntityQueryBuilder<K, E> createBooleanQuery(String field, String queryText, BooleanClause.Occur operator) throws Exception {
        checkProperty(field);
        query = builder.createBooleanQuery(field, queryText, operator);
        return this;
    }

    /**
     * Creates a phrase query from the query text.
     *
     * <p>This is equivalent to {@code createPhraseQuery(field, queryText, 0)}
     *
     * @param field     field name
     * @param queryText text to be passed to the analyzer
     * @return {@code TermQuery}, {@code BooleanQuery}, {@code PhraseQuery}, or {@code
     * MultiPhraseQuery}, based on the analysis of {@code queryText}
     */
    public EntityQueryBuilder<K, E> createPhraseQuery(String field, String queryText) throws Exception {
        checkProperty(field);
        query = builder.createPhraseQuery(field, queryText);
        return this;
    }

    /**
     * Creates a phrase query from the query text.
     *
     * @param field      field name
     * @param queryText  text to be passed to the analyzer
     * @param phraseSlop number of other words permitted between words in query phrase
     * @return {@code TermQuery}, {@code BooleanQuery}, {@code PhraseQuery}, or {@code
     * MultiPhraseQuery}, based on the analysis of {@code queryText}
     */
    public EntityQueryBuilder<K, E> createPhraseQuery(String field, String queryText, int phraseSlop) throws Exception {
        checkProperty(field);
        query = builder.createPhraseQuery(field, queryText, phraseSlop);
        return this;
    }

    /**
     * Creates a minimum-should-match query from the query text.
     *
     * @param field     field name
     * @param queryText text to be passed to the analyzer
     * @param fraction  of query terms {@code [0..1]} that should match
     * @return {@code TermQuery} or {@code BooleanQuery}, based on the analysis of {@code queryText}
     */
    public EntityQueryBuilder<K, E> createMinShouldMatchQuery(String field, String queryText, float fraction) {
        query = builder.createMinShouldMatchQuery(field, queryText, fraction);
        return this;
    }

    public AbstractDataStore.Q build() {
        return new LuceneQuery()
                .query(query)
                .where(query.toString());
    }
}
