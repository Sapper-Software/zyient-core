package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.audit.AuditLogger;
import ai.sapper.cdc.core.connections.MessageConnection;
import ai.sapper.cdc.core.processing.ProcessorState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class MessageSender<K, M> implements Closeable {
    private final ProcessorState state = new ProcessorState();
    private MessageConnection connection;
    private AuditLogger auditLogger;

    public MessageSender<K, M> withConnection(@NonNull MessageConnection connection) {
        Preconditions.checkArgument(connection.isConnected());
        Preconditions.checkArgument(connection.canSend());

        this.connection = connection;

        return this;
    }

    public MessageSender<K, M> withAuditLogger(AuditLogger auditLogger) {
        this.auditLogger = auditLogger;
        return this;
    }

    public abstract MessageSender<K, M> init() throws MessagingError;

    public abstract MessageObject<K, M> send(@NonNull MessageObject<K, M> message) throws MessagingError;

    public abstract List<MessageObject<K, M>> sent(@NonNull List<MessageObject<K, M>> messages) throws MessagingError;
}
