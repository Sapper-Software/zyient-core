package ai.sapper.cdc.core.executor;

import lombok.NonNull;

import java.util.concurrent.Future;

public interface CompletionCallback<T> {
    void finished(@NonNull BaseTask<T> task);

    void error(@NonNull BaseTask<T> task, @NonNull Throwable error);
}
