package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.messaging.MessageReceiver;
import ai.sapper.cdc.core.state.BaseStateManager;
import ai.sapper.cdc.entity.manager.SchemaManager;
import ai.sapper.cdc.entity.model.EntityReadState;
import ai.sapper.cdc.entity.model.TransactionId;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.UUID;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class Task<T extends TransactionId, I, M> implements Runnable, Closeable {
    private final TaskState state = new TaskState();

    private final String id;
    private final SchemaEntity entity;
    private TaskBatchResponse<T> response;
    private EntityReadState<T> entityState;
    private final MessageReceiver<I, M> receiver;
    private final BaseStateManager stateManager;
    private final SchemaManager schemaManager;
    private CompletionCallback<T> callback;

    public Task(@NonNull SchemaEntity entity,
                @NonNull MessageReceiver<I, M> receiver,
                @NonNull BaseStateManager stateManager,
                @NonNull SchemaManager schemaManager) {
        this.entity = entity;
        this.receiver = receiver;
        this.stateManager = stateManager;
        this.schemaManager = schemaManager;
        id = UUID.randomUUID().toString();
    }

    public Task<T, I, M> withCallback(@NonNull CompletionCallback<T> callback) {
        this.callback = callback;
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
                            String.format("Failed to create response instance. [entity=%s]", entity.toString()));
                }
                response.taskId(String.format("%s::%s", id, UUID.randomUUID().toString()));
                if (callback == null) {
                    throw new FatalError(String.format("Completion callback is null. [entity=%s]", entity.toString()));
                }
                response.start();
                try {
                    execute();
                    state.setState(ETaskState.DONE);
                    callback.finished(this);
                } finally {
                    response.close();
                }
            } catch (Throwable t) {
                if (response != null)
                    response.error(t);
                else
                    state.error(t);
                DefaultLogger.stacktrace(t);
                DefaultLogger.error(t.getLocalizedMessage());
                callback.error(this, t);
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

    public abstract void execute() throws Exception;
}
