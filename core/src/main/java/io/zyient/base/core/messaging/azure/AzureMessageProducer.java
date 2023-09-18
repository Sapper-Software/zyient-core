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

package io.zyient.base.core.messaging.azure;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.azure.ServiceBusProducerConnection;
import io.zyient.base.core.connections.settings.azure.AzureServiceBusConnectionSettings;
import io.zyient.base.core.messaging.MessageObject;
import io.zyient.base.core.messaging.MessageSender;
import io.zyient.base.core.messaging.MessagingError;
import io.zyient.base.core.processing.ProcessorState;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;

public abstract class AzureMessageProducer<M> extends MessageSender<String, M> {
    private ServiceBusProducerConnection producer;

    @Override
    public MessageSender<String, M> init() throws MessagingError {
        Preconditions.checkArgument(connection() instanceof ServiceBusProducerConnection);
        try {
            producer = (ServiceBusProducerConnection) connection();
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
        Preconditions.checkState(state().isAvailable());
        AzureServiceBusConnectionSettings settings = producer.settings();
        if (Strings.isNullOrEmpty(message.correlationId())) {
            message.correlationId(message.id());
        }
        message.queue(settings.getQueue());
        byte[] data = serialize(message.value());
        ServiceBusMessage m = new ServiceBusMessage(data)
                .setSubject(message.key())
                .setMessageId(message.id())
                .setCorrelationId(message.correlationId())
                .setSessionId(producer.sessionId());
        m.getApplicationProperties()
                .put(MessageObject.HEADER_MESSAGE_MODE, message.mode().name());
        producer.client().sendMessage(m);
        return message;
    }

    protected abstract byte[] serialize(@NonNull M message) throws MessagingError;

    @Override
    public List<MessageObject<String, M>> send(@NonNull List<MessageObject<String, M>> messages) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        List<MessageObject<String, M>> responses = new ArrayList<>(messages.size());
        for (MessageObject<String, M> message : messages) {
            MessageObject<String, M> m = send(message);
            responses.add(m);
        }
        return responses;
    }
}
