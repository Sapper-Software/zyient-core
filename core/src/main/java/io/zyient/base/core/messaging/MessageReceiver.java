/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.base.core.messaging;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.core.auditing.AbstractAuditLogger;
import io.zyient.base.core.connections.MessageConnection;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.Offset;
import io.zyient.base.core.state.OffsetState;
import io.zyient.base.core.state.OffsetStateManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class MessageReceiver<I, M> implements Closeable, AckDelegate<I> {
    private static final long DEFAULT_RECEIVE_TIMEOUT = 5000; // 5 secs default timeout.

    private final ProcessorState state = new ProcessorState();
    private MessageConnection connection;
    private int batchSize = 32;
    private AbstractAuditLogger<?> auditLogger;
    private OffsetStateManager<?> offsetStateManager;
    private boolean stateful = false;
    private long defaultReceiveTimeout = DEFAULT_RECEIVE_TIMEOUT;


    public MessageReceiver<I, M> withReceiveTimeout(long receiveTimeout) {
        Preconditions.checkArgument(receiveTimeout > 0);
        defaultReceiveTimeout = receiveTimeout;

        return this;
    }


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

    public MessageReceiver<I, M> withAuditLogger(AbstractAuditLogger<?> auditLogger) {
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

    public MessageObject<I, M> receive() throws MessagingError {
        return receive(defaultReceiveTimeout);
    }

    public abstract MessageObject<I, M> receive(long timeout) throws MessagingError;

    public List<MessageObject<I, M>> nextBatch() throws MessagingError {
        return nextBatch(defaultReceiveTimeout);
    }

    public abstract List<MessageObject<I, M>> nextBatch(long timeout) throws MessagingError;

    public abstract void ack(@NonNull List<I> messageIds) throws MessagingError;

    public abstract OffsetState<?, ?> currentOffset(Context context) throws MessagingError;

    public abstract void seek(@NonNull Offset offset, Context context) throws MessagingError;
}