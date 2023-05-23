package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.audit.AuditLogger;
import ai.sapper.cdc.core.connections.MessageConnection;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.core.state.OffsetStateManager;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class MessageReceiver<I, M> implements Closeable, AckDelegate<I> {
    private final ProcessorState state = new ProcessorState();
    private MessageConnection connection;
    private int batchSize = 32;
    private AuditLogger auditLogger;
    private OffsetStateManager<?> offsetStateManager;
    private boolean stateful = false;

    public MessageReceiver<I, M> withOffsetStateManager(OffsetStateManager<?> offsetStateManager) {
        this.offsetStateManager = offsetStateManager;
        stateful = true;
        return this;
    }

    public MessageReceiver<I, M> withConnection(@NonNull MessageConnection connection) {
        Preconditions.checkArgument(connection.isConnected());
        Preconditions.checkArgument(connection.canReceive());

        this.connection = connection;
        return this;
    }

    public MessageReceiver<I, M> withAuditLogger(AuditLogger auditLogger) {
        this.auditLogger = auditLogger;
        return this;
    }

    public MessageReceiver<I, M> withBatchSize(int batchSize) {
        if (batchSize > 0) {
            this.batchSize = batchSize;
        }
        return this;
    }

    public abstract MessageReceiver<I, M> init() throws MessagingError;

    public abstract MessageObject<I, M> receive() throws MessagingError;

    public abstract MessageObject<I, M> receive(long timeout) throws MessagingError;

    public abstract List<MessageObject<I, M>> nextBatch() throws MessagingError;

    public abstract List<MessageObject<I, M>> nextBatch(long timeout) throws MessagingError;

    public abstract void ack(@NonNull List<I> messageIds) throws MessagingError;
}
