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

package ai.sapper.cdc.core.index;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

@Getter
@Accessors(fluent = true)
public class SearchCursor implements Closeable {
    private final Directory baseDir;
    private final Query query;
    private final String id;
    private int readPage = 0;
    private int cachedPageStart = 0;
    private int cachedPageEnd = 0;
    private int pageSize = 256;
    private int pageBuffers = 2;
    private int bufferSize = pageSize * pageBuffers;
    private List<Document> cache = null;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private ScoreDoc lastReadDoc = null;

    public SearchCursor(@NonNull Directory baseDir,
                        @NonNull Query query) {
        this.baseDir = baseDir;
        this.query = query;
        id = UUID.randomUUID().toString();
    }

    public SearchCursor withPageSize(int pageSize) {
        Preconditions.checkState(cache == null);
        if (pageSize > 0) {
            this.pageSize = pageSize;
            bufferSize = this.pageSize * this.pageBuffers;
        }
        return this;
    }

    public SearchCursor withPageBuffers(int pageBuffers) {
        Preconditions.checkState(cache == null);
        if (pageBuffers > 0) {
            this.pageBuffers = pageBuffers;
            bufferSize = this.pageSize * this.pageBuffers;
        }
        return this;
    }

    public SearchCursor create(@NonNull Executor executor) throws Exception {
        cache = new ArrayList<>(bufferSize);
        reader = DirectoryReader.open(baseDir);
        searcher = new IndexSearcher(reader, executor);
        return this;
    }

    public Collection<Document> fetch() throws Exception {
        Preconditions.checkNotNull(searcher);
        if (readPage < cachedPageEnd) {
            int start = (readPage - cachedPageStart) * pageSize;
            readPage++;
            return fromCache(start, pageSize);
        } else if (cache.isEmpty()) {
            TopDocs topDocs = searcher.search(query, bufferSize);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.doc(scoreDoc.doc);
                cache.add(doc);
                lastReadDoc = scoreDoc;
            }
            double count = ((double) cache.size()) / pageSize;
            cachedPageEnd = (int) Math.ceil(count);
            readPage++;
            return fromCache(0, pageSize);
        } else if (cache.size() == bufferSize) {
            cache.clear();
            if (lastReadDoc != null) {
                cachedPageStart = cachedPageEnd;
                TopDocs topDocs = searcher.searchAfter(lastReadDoc, query, bufferSize);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    Document doc = searcher.doc(scoreDoc.doc);
                    cache.add(doc);
                    lastReadDoc = scoreDoc;
                }
                double count = ((double) cache.size()) / pageSize;
                cachedPageEnd += (int) Math.ceil(count);

                readPage++;
                return fromCache(0, pageSize);
            }
        }
        return null;
    }

    public void reset() {
        if (cache != null) {
            cache.clear();
        }
        lastReadDoc = null;
        readPage = 0;
        cachedPageStart = 0;
        cachedPageEnd = 0;
    }

    private Collection<Document> fromCache(int start, int size) {
        if (start >= cache.size()) return null;
        int end = start + size;
        if (end > cache.size()) {
            end = cache.size();
        }
        return cache.subList(start, end);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        searcher = null;
    }
}
