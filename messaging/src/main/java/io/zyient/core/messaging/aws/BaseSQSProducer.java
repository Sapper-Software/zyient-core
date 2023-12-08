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

package io.zyient.core.messaging.aws;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.messaging.MessagingError;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.aws.AwsSQSProducerConnection;
import io.zyient.base.core.connections.settings.aws.AwsSQSConnectionSettings;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.messaging.MessageObject;
import io.zyient.core.messaging.MessageSender;
import lombok.NonNull;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseSQSProducer<M> extends MessageSender<String, M> {
    private AwsSQSProducerConnection producer;
    private String queueUrl;

    @Override
    public MessageSender<String, M> init() throws MessagingError {
        try {
            Preconditions.checkState(connection() instanceof AwsSQSProducerConnection);
            producer = (AwsSQSProducerConnection) connection();
            AwsSQSConnectionSettings settings = (AwsSQSConnectionSettings) producer.settings();
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(settings.getQueue())
                    .build();
            queueUrl = producer.getClient().getQueueUrl(getQueueRequest).queueUrl();
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
        AwsSQSConnectionSettings settings = (AwsSQSConnectionSettings) producer.settings();
        if (Strings.isNullOrEmpty(message.correlationId())) {
            message.correlationId(message.id());
        }
        message.queue(settings.getQueue());
        String mr = serialize(message.value());
        final Map<String, MessageAttributeValue> attributes = new HashMap<>();
        attributes.put(MessageObject.HEADER_MESSAGE_ID, MessageAttributeValue.builder()
                .stringValue(message.id())
                .build());
        attributes.put(MessageObject.HEADER_CORRELATION_ID, MessageAttributeValue.builder()
                .stringValue(message.correlationId())
                .build());
        attributes.put(MessageObject.HEADER_MESSAGE_MODE, MessageAttributeValue.builder()
                .stringValue(message.mode().name())
                .build());
        attributes.put(SQSMessage.HEADER_MESSAGE_KEY, MessageAttributeValue.builder()
                .stringValue(message.key())
                .build());
        attributes.put(SQSMessage.HEADER_MESSAGE_TIMESTAMP, MessageAttributeValue.builder()
                .stringValue(String.valueOf(System.nanoTime()))
                .build());
        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageAttributes(attributes)
                .messageBody(mr)
                .delaySeconds(5)
                .build();
        producer.getClient().sendMessage(sendMsgRequest);
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

    protected abstract String serialize(@NonNull M message) throws MessagingError;

    @Override
    public void close() throws IOException {
        if (state().isAvailable()) {
            state().setState(ProcessorState.EProcessorState.Stopped);
        }
    }
}
