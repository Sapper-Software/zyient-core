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

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.DirectoryCleaner;
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
    private DirectoryCleaner dirCleaner;
    private Thread cleanerThread;

    @Override
    public MessageSender<String, M> init() throws MessagingError {
        try {
            Preconditions.checkState(connection() instanceof ChronicleProducerConnection);
            producer = (ChronicleProducerConnection) connection();
            dirCleaner = new DirectoryCleaner(producer.messageDir(),
                    true,
                    producer.settings().getCleanUpTTL().normalized(),
                    60 * 1000);
            cleanerThread = new Thread(dirCleaner,
                    String.format("QUEUE-%s-CLEANER", producer.settings().getName()));
            cleanerThread.start();

            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new MessagingError(ex);
        }
    }

    @Override
    public MessageObject<String, M> send(@NonNull MessageObject<String, M> message) throws MessagingError {
        Preconditions.checkArgument(state().isAvailable());
        if (Strings.isNullOrEmpty(message.correlationId())) {
            message.correlationId(message.id());
        }
        message.queue(producer.settings().getQueue());
        byte[] data = serialize(message.value());
        MessageEnvelop envelop = new MessageEnvelop(message, data)
                .queue(message.queue());

        producer.appender().writeDocument(w -> w.write(producer.settings().getName()).marshallable(envelop));
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
        if (dirCleaner != null) {
            try {
                dirCleaner.stop();
                cleanerThread.join();
            } catch (Exception ex) {
                DefaultLogger.error(ex.getLocalizedMessage());
            }
        }
    }
}
