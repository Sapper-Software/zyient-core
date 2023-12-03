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

package io.zyient.base.core.content;

import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.content.model.Document;
import io.zyient.base.core.content.model.DocumentId;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.io.Reader;
import io.zyient.base.core.io.model.FileInode;
import io.zyient.base.core.io.model.PathInfo;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.impl.solr.SolrCursor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.File;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class SolrContentCursor<E extends Enum<?>, K extends IKey> extends SolrCursor<DocumentId, Document<E, K>>
        implements ContentCursor<E, K> {
    private final FileSystem fileSystem;
    private boolean download = false;

    public SolrContentCursor(@NonNull SolrCursor<DocumentId, Document<E, K>> cursor,
                             @NonNull FileSystem fileSystem) {
        super(cursor.entityType(), cursor.client(), cursor.query(), cursor.batchSize());
        this.fileSystem = fileSystem;
    }

    @Override
    protected List<Document<E, K>> next(int page) throws DataStoreException {
        List<Document<E, K>> docs = super.next(page);
        if (download) {
            for (Document<E, K> doc : docs) {
                fetch(doc);
            }
        }
        return docs;
    }

    @SuppressWarnings("unchecked")
    public Document<E, K> fetch(@NonNull Document<E, K> doc) throws DataStoreException {
        try {
            Map<String, String> map = JSONUtils.read(doc.getUri(), Map.class);
            PathInfo pi = fileSystem.parsePathInfo(map);
            FileInode fi = (FileInode) fileSystem.getInode(pi);
            if (fi == null) {
                throw new DataStoreException(String.format("Document not found. [uri=%s]", doc.getUri()));
            }
            try (Reader reader = fileSystem.reader(pi)) {
                File path = reader.copy();
                doc.setPath(path);
            }
            return doc;
        } catch (Exception ex) {
            throw new DataStoreException(ex);
        }
    }
}
