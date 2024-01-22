/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.messaging.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.messaging.MessagingError;
import io.zyient.base.core.connections.kafka.BasicKafkaProducerConnection;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.messaging.MessageObject;
import io.zyient.core.messaging.MessageSender;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class BaseKafkaProducer<M> extends MessageSender<String, M> {
    @Setter(AccessLevel.NONE)
    private BasicKafkaProducerConnection producer;
    @Setter(AccessLevel.NONE)
    private String topic;
    private KafkaPartitioner<M> partitioner;

    @Override
    public MessageSender<String, M> init() throws MessagingError {
        Preconditions.checkArgument(connection() instanceof BasicKafkaProducerConnection);
        producer = (BasicKafkaProducerConnection) connection();
        topic = ((BasicKafkaProducerConnection) connection()).topic();
        state().setState(ProcessorState.EProcessorState.Running);
        return this;
    }

    @Override
    public MessageObject<String, M> send(@NonNull MessageObject<String, M> message) throws MessagingError {
        Preconditions.checkArgument(state().isAvailable());
        try {
            message.queue(topic);

            List<Header> headers = new ArrayList<>();
            Header h = new RecordHeader(MessageObject.HEADER_MESSAGE_ID, message.id().getBytes(StandardCharsets.UTF_8));
            headers.add(h);
            if (!Strings.isNullOrEmpty(message.correlationId())) {
                h = new RecordHeader(MessageObject.HEADER_CORRELATION_ID, message.correlationId().getBytes(StandardCharsets.UTF_8));
                headers.add(h);
            }
            if (message.mode() == null) {
                throw new MessagingError(String.format("Invalid Message Object: mode not set. [id=%s]", message.id()));
            }
            h = new RecordHeader(MessageObject.HEADER_MESSAGE_MODE, message.mode().name().getBytes(StandardCharsets.UTF_8));
            headers.add(h);

            byte[] data = serialize(message.value());
            Future<RecordMetadata> result = null;
            Integer partition = null;
            if (partitioner != null) {
                partition = partitioner.partition(message.value());
            }
            result = producer.producer().send(new ProducerRecord<>(topic, partition, message.key(), data, headers));
            RecordMetadata rm = result.get();

            return message;
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
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
