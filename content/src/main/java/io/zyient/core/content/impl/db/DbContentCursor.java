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

package io.zyient.core.content.impl.db;

import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.base.core.stores.impl.rdbms.HibernateCursor;
import io.zyient.base.core.stores.model.Document;
import io.zyient.base.core.stores.model.DocumentId;
import io.zyient.base.core.stores.model.DocumentState;
import io.zyient.core.content.ContentCursor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class DbContentCursor<E extends DocumentState<?>, K extends IKey, D extends Document<E, K, D>> extends HibernateCursor<DocumentId, Document<E, K, D>>
        implements ContentCursor<E, K, D> {
    private final FileSystem fileSystem;
    private boolean download = false;

    protected DbContentCursor(@NonNull HibernateCursor<DocumentId, Document<E, K, D>> cursor,
                              @NonNull FileSystem fileSystem) {
        super(cursor);
        this.fileSystem = fileSystem;
    }

    @Override
    protected List<Document<E, K, D>> next(int page) throws DataStoreException {
        List<Document<E, K, D>> docs = super.next(page);
        if (download) {
            for (Document<E, K, D> doc : docs) {
                fetch(doc);
            }
        }
        return docs;
    }

    @Override
    public Document<E, K, D> fetch(@NonNull Document<E, K, D> doc) throws DataStoreException {
        return ContentCursor.fetch(doc, fileSystem);
    }

}
