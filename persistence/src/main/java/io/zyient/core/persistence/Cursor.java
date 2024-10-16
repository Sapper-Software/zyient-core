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

package io.zyient.core.persistence;

import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class Cursor<K extends IKey, E extends IEntity<K>> implements Closeable {
    private int pageSize = 256;
    @Setter(AccessLevel.NONE)
    private int currentPage = 0;
    private boolean EOF = false;

    protected Cursor(int currentPage) {
        if (currentPage < 0) {
            currentPage = 0;
        }
        this.currentPage = currentPage;
    }

    public List<E> nextPage() throws DataStoreException {
        if (!EOF) {
            List<E> result = next(currentPage);
            if (result != null) {
                if (result.size() < pageSize) {
                    EOF = true;
                } else
                    currentPage++;
            } else {
                EOF = true;
            }
            return result;
        }
        return null;
    }

    protected abstract List<E> next(int page) throws DataStoreException;
}
