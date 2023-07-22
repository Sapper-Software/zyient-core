package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.entity.model.TransactionId;
import lombok.NonNull;

public interface MessageCompletionCallback<T extends TransactionId> {
    void finished(@NonNull MessageTask<T, ?, ?> task);

    void error(@NonNull MessageTask<T, ?, ?> task, @NonNull Throwable error);
}
