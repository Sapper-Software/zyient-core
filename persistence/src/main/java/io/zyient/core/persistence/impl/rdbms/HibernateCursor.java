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

package io.zyient.core.persistence.impl.rdbms;

import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.core.persistence.Cursor;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.core.persistence.model.BaseEntity;
import lombok.NonNull;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HibernateCursor<K extends IKey, E extends IEntity<K>> extends Cursor<K, E> {
    private final ScrollableResults<E> results;
    private final Session session;

    public HibernateCursor(@NonNull Session session,
                           @NonNull ScrollableResults<E> results,
                           int currentPage) {
        super(currentPage);
        this.session = session;
        this.results = results;
    }

    public HibernateCursor(@NonNull HibernateCursor<K, E> cursor) {
        super(cursor.currentPage());
        session = cursor.session;
        results = cursor.results;
    }

    @Override
    protected List<E> next(int page) throws DataStoreException {
        int index = page * pageSize() + 1;
        if (!results.position(index)) {
            return null;
        }
        List<E> batch = new ArrayList<>();
        int count = pageSize();
        while (count > 0) {
            E entity = results.get();
            if (entity instanceof BaseEntity<?>) {
                ((BaseEntity<?>) entity).getState().setState(EEntityState.Synced);
            }
            batch.add(entity);
            if (!results.next()) {
                break;
            }
            count--;
        }
        return batch;
    }

    @Override
    public void close() throws IOException {
        results.close();
        //session.close();
    }
}
