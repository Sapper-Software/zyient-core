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

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.state.BaseStateManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class BaseTask<T> implements Runnable, Closeable {
    private final TaskState state = new TaskState();

    private final String type;
    private final String id;
    private int shardId = 0;
    private TaskResponse<T> response;
    private final BaseStateManager stateManager;
    private final List<CompletionCallback<T>> callbacks = new ArrayList<>();

    public BaseTask(@NonNull BaseStateManager stateManager,
                    @NonNull String type) {
        this.stateManager = stateManager;
        this.type = type;
        id = String.format("%s::%s", type, UUID.randomUUID().toString());
    }

    public BaseTask(@NonNull BaseStateManager stateManager,
                    @NonNull String type,
                    @NonNull String key) {
        this.stateManager = stateManager;
        this.type = type;
        id = String.format("%s::%s::%s", type, key, UUID.randomUUID().toString());
    }

    public BaseTask<T> withResponse(@NonNull TaskResponse<T> response) {
        this.response = response;
        return this;
    }

    public BaseTask<T> withCallback(@NonNull CompletionCallback<T> callback) {
        callbacks.add(callback);
        return this;
    }

    /**
     *
     */
    @Override
    public void run() {
        synchronized (this) {
            state.setState(ETaskState.RUNNING);
            try {
                if (response == null)
                    response = initResponse();
                if (response == null) {
                    throw new FatalError(
                            String.format("Failed to create response instance. [entity=%s]", id));
                }
                response.taskId(id);
                if (callbacks.isEmpty()) {
                    throw new FatalError(String.format("Completion callback is null. [entity=%s]", id));
                }
                response.start();
                try {
                    T result = execute();
                    response.result(result);
                    state.setState(ETaskState.DONE);
                    for (CompletionCallback<T> callback : callbacks)
                        callback.finished(this, response);
                    response.error(null);
                    response.state(ETaskState.DONE);
                } finally {
                    response.close();
                }
            } catch (Throwable t) {
                if (response != null) {
                    response.state(ETaskState.ERROR);
                    response.error(t);
                } else
                    state.error(t);
                DefaultLogger.stacktrace(t);
                DefaultLogger.error(t.getLocalizedMessage());
                for (CompletionCallback<T> callback : callbacks)
                    callback.error(this, t, response);
            } finally {
                notifyAll();
            }
        }
    }

    public void stop() {
        if (state.getState() != ETaskState.ERROR) {
            state.setState(ETaskState.STOPPED);
        }
    }

    public abstract TaskResponse<T> initResponse();

    public abstract T execute() throws Exception;
}
