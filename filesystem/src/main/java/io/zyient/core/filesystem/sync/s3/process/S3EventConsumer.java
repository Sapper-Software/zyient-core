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

package io.zyient.core.filesystem.sync.s3.process;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.messaging.MessagingError;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.connections.aws.AwsSQSConsumerConnection;
import io.zyient.core.filesystem.sync.s3.model.S3Event;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class S3EventConsumer {
    private static class MessageHandle {
        private String id;
        private String receiptId;
        private Message message;
        private boolean acked = false;
    }

    private final AwsSQSConsumerConnection connection;
    private final String queueUrl;
    private final int batchSize;
    private final long timeout;
    private final int ackTimeout;
    private final Map<String, MessageHandle> messages = new HashMap<>();

    public S3EventConsumer(@NonNull AwsSQSConsumerConnection connection,
                           @NonNull String queueUrl,
                           int batchSize,
                           long timeout,
                           int ackTimeout) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(queueUrl));
        Preconditions.checkArgument(batchSize > 0);
        Preconditions.checkArgument(timeout > 0);
        Preconditions.checkArgument(ackTimeout > 0);

        this.connection = connection;
        this.queueUrl = queueUrl;
        if (batchSize > 10) {
            batchSize = 10;
        }
        this.batchSize = batchSize;
        this.timeout = timeout;
        this.ackTimeout = ackTimeout;
        if (!connection.isConnected()) {
            connection.connect();
        }
    }

    public void ack(@NonNull S3Event event) throws MessagingError {
        if (messages.containsKey(event.getMessageId())) {
            MessageHandle handle = messages.get(event.getMessageId());
            handle.acked = true;
        } else {
            throw new MessagingError(String.format("Message not found. [id=%s]", event.getMessageId()));
        }
    }

    @SuppressWarnings("unchecked")
    public List<S3Event> next() throws MessagingError {
        try {
            int t = (int) TimeUnit.MILLISECONDS.toSeconds(timeout);
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .visibilityTimeout(ackTimeout)
                    .waitTimeSeconds(t)
                    .maxNumberOfMessages(batchSize())
                    .build();
            List<Message> records = connection.getClient().receiveMessage(receiveRequest).messages();
            if (records != null && !records.isEmpty()) {
                List<S3Event> events = new ArrayList<>(records.size());
                synchronized (messages) {
                    for (Message record : records) {
                        MessageHandle m = new MessageHandle();
                        m.id = record.messageId();
                        m.receiptId = record.receiptHandle();
                        m.message = record;
                        String body = record.body();
                        if (!Strings.isNullOrEmpty(body)) {
                            Map<String, Object> data = JSONUtils.read(body, Map.class);
                            List<S3Event> es = S3Event.read(data);
                            if (es != null) {
                                for (S3Event event : es) {
                                    event.setMessageId(m.id);
                                    events.add(event);
                                }
                            }
                        } else {
                            DefaultLogger.warn(String.format("[id=%s] Empty message received...", m.id));
                            m.acked = true;
                        }
                        messages.put(m.id, m);
                    }
                }
                return events;
            }
            return null;
        } catch (Throwable t) {
            throw new MessagingError(t);
        }
    }

    public void commit() throws MessagingError {
        try {
            synchronized (messages) {
                if (!messages.isEmpty()) {
                    List<MessageHandle> delete = new ArrayList<>();
                    List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
                    for (MessageHandle handle : messages.values()) {
                        if (handle.acked) {
                            DeleteMessageBatchRequestEntry entry = DeleteMessageBatchRequestEntry.builder()
                                    .id(handle.id)
                                    .receiptHandle(handle.receiptId)
                                    .build();
                            entries.add(entry);
                            delete.add(handle);
                        }
                    }
                    if (!entries.isEmpty()) {
                        DeleteMessageBatchRequest request = DeleteMessageBatchRequest.builder()
                                .entries(entries)
                                .queueUrl(queueUrl)
                                .build();
                        DeleteMessageBatchResponse response = connection.client().deleteMessageBatch(request);
                        if (response.hasFailed()) {
                            List<BatchResultErrorEntry> errors = response.failed();
                            for (BatchResultErrorEntry error : errors) {
                                DefaultLogger.error(String.format("[queue=%s] Delete failed for message: [id=%s][code=%s]",
                                        queueUrl, error.id(), error.code()));
                            }
                            DefaultLogger.error(String.format("[queue=%s] Delete failed count = %d",
                                    queueUrl, errors.size()));
                        }
                    }
                    if (!delete.isEmpty()) {
                        for (MessageHandle handle : delete) {
                            messages.remove(handle.id);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new MessagingError(t);
        }
    }
}
