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

    public abstract List<MessageObject<K, M>> send(@NonNull List<MessageObject<K, M>> messages) throws MessagingError;
}
