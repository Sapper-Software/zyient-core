package ai.sapper.cdc.core.executor;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.state.BaseStateManager;
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
    private TaskBatchResponse<T> response;
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

    public abstract TaskBatchResponse<T> initResponse();

    public abstract T execute() throws Exception;
}
