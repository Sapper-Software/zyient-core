package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.entity.model.TransactionId;
import lombok.NonNull;

public interface CompletionCallback<T extends TransactionId> {
    void finished(@NonNull Task<T, ?, ?> task);

    void error(@NonNull Task<T, ?, ?> task, @NonNull Throwable error);
}
