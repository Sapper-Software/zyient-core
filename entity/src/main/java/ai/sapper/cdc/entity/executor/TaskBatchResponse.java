package ai.sapper.cdc.entity.executor;

import ai.sapper.cdc.entity.model.EntityReadState;
import ai.sapper.cdc.entity.model.TransactionId;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public class TaskBatchResponse<T extends TransactionId> implements Closeable {
    private String taskId;
    private int batchSize;
    private long timestamp;
    private long startTime = -1;
    @Setter(AccessLevel.NONE)
    private long execTime;
    private EntityReadState<T> state;
    private Throwable error;

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
}
