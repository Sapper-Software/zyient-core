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

package io.zyient.core.mapping.readers;

import io.zyient.core.mapping.model.mapping.SourceMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@Accessors(fluent = true)
public class ReadCursor implements Closeable {
    private final InputReader reader;
    private final LinkedBlockingQueue<SourceMap> cached;
    private boolean EOF = false;

    public ReadCursor(@NonNull InputReader reader, int batchSize) {
        this.reader = reader;
        cached = new LinkedBlockingQueue<>(batchSize);
    }

    public SourceMap next() throws IOException {
        if (EOF) return null;
        if (cached.isEmpty()) {
            List<SourceMap> batch = reader.nextBatch();
            if (batch == null || batch.isEmpty()) {
                EOF = true;
                return null;
            }
            cached.addAll(batch);
        }
        return cached.poll();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
