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

package ai.sapper.cdc.core.messaging.chronicle;

import ai.sapper.cdc.core.connections.chronicle.ChronicleProducerConnection;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.messaging.MessagingError;
import ai.sapper.cdc.core.processing.ProcessorState;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class BaseChronicleProducer<M> extends MessageSender<String, M> {
    private ChronicleProducerConnection producer;

    @Override
    public MessageSender<String, M> init() throws MessagingError {
        Preconditions.checkState(connection() instanceof ChronicleProducerConnection);
        producer = (ChronicleProducerConnection) connection();
        state().setState(ProcessorState.EProcessorState.Running);
        return this;
    }

    @Override
    public MessageObject<String, M> send(@NonNull MessageObject<String, M> message) throws MessagingError {
        Preconditions.checkArgument(state().isAvailable());
        if (Strings.isNullOrEmpty(message.correlationId())) {
            message.correlationId(message.id());
        }
        if (Strings.isNullOrEmpty(message.queue())) {
            message.queue(producer.settings().getName());
        }
        byte[] data = serialize(message.value());
        producer.appender().writeDocument(w -> w.write(producer.settings().getName()).marshallable(m ->
                m.write(BaseChronicleMessage.HEADER_MESSAGE_ID).text(message.id())
                        .write(BaseChronicleMessage.HEADER_CORRELATION_ID).text(message.correlationId())
                        .write(BaseChronicleMessage.HEADER_MESSAGE_MODE).text(message.mode().name())
                        .write(BaseChronicleMessage.HEADER_MESSAGE_KEY).text(message.key())
                        .write(BaseChronicleMessage.HEADER_MESSAGE_QUEUE).text(message.queue())
                        .write(BaseChronicleMessage.HEADER_MESSAGE_BODY).bytes(data)));
        return message;
    }

    @Override
    public List<MessageObject<String, M>> send(@NonNull List<MessageObject<String, M>> messages) throws MessagingError {
        Preconditions.checkArgument(state().isAvailable());
        List<MessageObject<String, M>> responses = new ArrayList<>(messages.size());
        for (MessageObject<String, M> message : messages) {
            MessageObject<String, M> response = send(message);
            responses.add(response);
        }
        return responses;
    }

    protected abstract byte[] serialize(@NonNull M message) throws MessagingError;

    @Override
    public void close() throws IOException {
        if (state().isAvailable()) {
            state().setState(ProcessorState.EProcessorState.Stopped);
        }
    }
}
