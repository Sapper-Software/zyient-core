package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.core.executor.BaseTask;
import ai.sapper.cdc.core.executor.CompletionCallback;
import ai.sapper.cdc.core.executor.TaskState;
import ai.sapper.cdc.core.messaging.MessageReceiver;
import ai.sapper.cdc.core.state.BaseStateManager;
import ai.sapper.cdc.entity.manager.SchemaManager;
import ai.sapper.cdc.entity.model.EntityReadState;
import ai.sapper.cdc.entity.model.TransactionId;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class MessageTask<T extends TransactionId, I, M> extends EntityTask<T> {
    private final TaskState state = new TaskState();

    private final SchemaEntity entity;
    private EntityReadState<T> entityState;
    private final MessageReceiver<I, M> receiver;
    private final SchemaManager schemaManager;

    public MessageTask(@NonNull String type,
                       @NonNull SchemaEntity entity,
                       @NonNull MessageReceiver<I, M> receiver,
                       @NonNull BaseStateManager stateManager,
                       @NonNull SchemaManager schemaManager) {
        super(stateManager, schemaManager, type, entity);
        this.entity = entity;
        this.receiver = receiver;
        this.schemaManager = schemaManager;
    }

    public MessageTask(@NonNull SchemaEntity entity,
                       @NonNull MessageReceiver<I, M> receiver,
                       @NonNull BaseStateManager stateManager,
                       @NonNull SchemaManager schemaManager) {
        super(stateManager, schemaManager, entity);
        this.entity = entity;
        this.receiver = receiver;
        this.schemaManager = schemaManager;
    }

    @Override
    public BaseTask<T> withCallback(@NonNull CompletionCallback<T> callback) {
        Preconditions.checkArgument(callback instanceof MessageCompletionCallback<?>);
        return super.withCallback(callback);
    }
}
