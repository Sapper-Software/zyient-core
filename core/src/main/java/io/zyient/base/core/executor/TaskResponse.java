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

package io.zyient.base.core.executor;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public class TaskResponse<T> implements Closeable {
    private String taskId;
    private long timestamp;
    private long startTime = -1;
    @Setter(AccessLevel.NONE)
    private long execTime;
    private Throwable error;
    private T result;
    private ETaskState state = ETaskState.UNKNOWN;

    public long start() {
        return (startTime = System.currentTimeMillis());
    }

    /**
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (startTime < 0) {
            throw new IOException("Batch not started...");
        }
        execTime = System.currentTimeMillis() - startTime;
    }

    public TaskResponse<T> markError(@NonNull Throwable error) {
        this.error = error;
        this.state = ETaskState.ERROR;
        return this;
    }

    public boolean hasError() {
        return state == ETaskState.ERROR;
    }
}
