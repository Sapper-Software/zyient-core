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

package io.zyient.base.core.auditing;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.StateException;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.keystore.KeyStore;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AuditCursor<T extends AuditRecord<R>, R> implements Closeable {
    public enum AuditCursorState {
        Unknown, Opened, Closed, EOF;
    }

    private int pageSize = 128;
    @Setter(AccessLevel.NONE)
    private int currentPage = 0;
    @Setter(AccessLevel.NONE)
    protected AuditCursorState state = AuditCursorState.Unknown;
    private Class<? extends T> recordType;
    private EncryptionInfo encryption;
    private KeyStore keyStore;
    private IAuditSerDe<?> serializer;


    protected AuditCursor(int currentPage) {
        if (currentPage < 0) {
            currentPage = 0;
        }
        this.currentPage = currentPage;
    }

    protected AuditCursor() {
    }


    public AuditCursor<T, R> open() throws Exception {
        if (recordType == null) {
            throw new Exception("Audit Record type not specified...");
        }
        state = AuditCursorState.Opened;
        return this;
    }

    public List<T> next() throws Exception {
        if (state == AuditCursorState.EOF) return null;
        if (state != AuditCursorState.Opened) {
            throw new StateException(String.format("Cursor is not available. [state=%s]", state.name()));
        }
        List<T> next = next(currentPage);
        if (next == null || next.size() < pageSize) {
            state = AuditCursorState.EOF;
        } else {
            currentPage++;
        }
        return next;
    }

    protected abstract List<T> next(int page) throws Exception;
}
