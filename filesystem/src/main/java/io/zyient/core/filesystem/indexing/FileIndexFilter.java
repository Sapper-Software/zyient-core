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

package io.zyient.core.filesystem.indexing;

import com.google.common.base.Preconditions;
import io.zyient.core.filesystem.model.FileState;
import io.zyient.core.filesystem.model.InodeType;
import lombok.NonNull;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.util.Date;

public class FileIndexFilter {
    private final BooleanQuery.Builder builder = new BooleanQuery.Builder();

    public Query build() {
        return builder.build();
    }

    public Query and(@NonNull InodeIndexConstants.InodeQuery term,
                     @NonNull Object value) throws Exception {
        Query q = getQuery(term, value);
        builder.add(new BooleanClause(q, BooleanClause.Occur.MUST));
        return q;
    }

    public Query or(@NonNull InodeIndexConstants.InodeQuery term,
                    @NonNull Object value) throws Exception {
        Query q = getQuery(term, value);
        builder.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
        return q;
    }

    public Query and(@NonNull Query query) {
        builder.add(new BooleanClause(query, BooleanClause.Occur.MUST));
        return query;
    }

    public Query or(@NonNull Query query) {
        builder.add(new BooleanClause(query, BooleanClause.Occur.SHOULD));
        return query;
    }

    public Query not(@NonNull InodeIndexConstants.InodeQuery term,
                     @NonNull Object value) throws Exception {
        Query q = getQuery(term, value);
        builder.add(new BooleanClause(q, BooleanClause.Occur.MUST_NOT));
        return q;
    }

    public Query name(@NonNull String name) throws Exception {
        return and(InodeIndexConstants.InodeQuery.NAME, name);
    }

    public Query fileSystemPath(@NonNull String path) throws Exception {
        return and(InodeIndexConstants.InodeQuery.FS_PATH, path);
    }

    public Query zkPath(@NonNull String path) throws Exception {
        return and(InodeIndexConstants.InodeQuery.ZK_PATH, path);
    }

    public Query absolutePath(@NonNull String path) throws Exception {
        return and(InodeIndexConstants.InodeQuery.ABSOLUTE_PATH, path);
    }


    public Query domain(@NonNull String name) throws Exception {
        return and(InodeIndexConstants.InodeQuery.DOMAIN, name);
    }


    public Query uuid(@NonNull String uuid) throws Exception {
        return and(InodeIndexConstants.InodeQuery.NAME, uuid);
    }


    public Query type(@NonNull InodeType type) throws Exception {
        return and(InodeIndexConstants.InodeQuery.TYPE, type);
    }

    public Query state(@NonNull FileState state) throws Exception {
        return and(InodeIndexConstants.InodeQuery.STATE, state);
    }

    public Query created(@NonNull Date date) throws Exception {
        long ts = date.getTime();
        return and(InodeIndexConstants.InodeQuery.CREATED, ts);
    }

    public Query created(Date start, Date end) throws Exception {
        Preconditions.checkArgument(start != null || end != null);
        long ds = 0;
        long de = Long.MAX_VALUE;
        if (start != null) {
            ds = start.getTime();
        }
        if (end != null) {
            de = end.getTime();
        }
        return and(InodeIndexConstants.InodeQuery.CREATED, new Long[]{ds, de});
    }

    public Query modified(@NonNull Date date) throws Exception {
        long ts = date.getTime();
        return and(InodeIndexConstants.InodeQuery.MODIFIED, ts);
    }

    public Query modified(Date start, Date end) throws Exception {
        Preconditions.checkArgument(start != null || end != null);
        long ds = 0;
        long de = Long.MAX_VALUE;
        if (start != null) {
            ds = start.getTime();
        }
        if (end != null) {
            de = end.getTime();
        }
        return and(InodeIndexConstants.InodeQuery.MODIFIED, new Long[]{ds, de});
    }

    private Query getQuery(@NonNull InodeIndexConstants.InodeQuery term,
                           @NonNull Object value) throws Exception {
        Term t;
        Query q = null;
        switch (term) {
            case NAME, PATH, FS_PATH, DOMAIN, ZK_PATH, ABSOLUTE_PATH -> {
                if (!(value instanceof String v)) {
                    throw new Exception(String.format("Invalid query term. [term=%s][value=%s]", term.name(), value));
                }
                t = new Term(term.term(), v);
                if (v.indexOf('*') >= 0 || v.indexOf('?') >= 0) {
                    q = new WildcardQuery(t);
                } else {
                    q = new TermQuery(t);
                }
            }
            case TYPE -> {
                if (!(value instanceof InodeType type)) {
                    throw new Exception(String.format("Invalid query term. [term=%s][value=%s]", term.name(), value));
                }
                t = new Term(term.term(), type.name());
                q = new TermQuery(t);
            }
            case UUID -> {
                if (!(value instanceof String u)) {
                    throw new Exception(String.format("Invalid query term. [term=%s][value=%s]", term.name(), value));
                }
                t = new Term(term.term(), u);
                q = new TermQuery(t);
            }
            case STATE -> {
                if (!(value instanceof FileState s)) {
                    throw new Exception(String.format("Invalid query term. [term=%s][value=%s]", term.name(), value));
                }
                t = new Term(term.term(), s.getState().name());
                q = new TermQuery(t);
            }
            case CREATED, MODIFIED -> {
                if (value instanceof Long) {
                    q = LongPoint.newExactQuery(term.term(), (long) value);
                } else if (value.getClass().isArray()) {
                    Long[] range = (Long[]) value;
                    long min = Long.MIN_VALUE;
                    long max = Long.MAX_VALUE;
                    if (range[0] != null) {
                        min = range[0];
                    }
                    if (range.length > 1 && range[1] != null) {
                        max = range[1];
                    }
                    q = LongRange.newContainsQuery(term.term(), new long[]{min}, new long[]{max});
                } else {
                    throw new Exception(String.format("Invalid query term. [term=%s][value=%s]", term.name(), value));
                }
            }
        }
        if (q == null) {
            throw new Exception(String.format("Failed to parse query. [term=%s][value=%s]", term.name(), value));
        }
        return q;
    }
}
