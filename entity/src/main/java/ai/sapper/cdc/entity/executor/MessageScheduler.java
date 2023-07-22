package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.core.executor.BaseScheduler;
import ai.sapper.cdc.core.executor.BaseTask;
import ai.sapper.cdc.entity.model.TransactionId;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class MessageScheduler<T extends TransactionId> extends BaseScheduler<T> {
    @Override
    public void add(@NonNull BaseTask<T> task) {
        Preconditions.checkArgument(task instanceof MessageTask<?, ?, ?>);
        super.add(task);
    }
}
