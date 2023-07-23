package ai.sapper.cdc.core.executor;

import lombok.NonNull;

public interface CompletionCallback<T> {
    void finished(@NonNull BaseTask<T> task, @NonNull TaskResponse<T> response);

    void error(@NonNull BaseTask<T> task, @NonNull Throwable error, TaskResponse<T> response);
}
