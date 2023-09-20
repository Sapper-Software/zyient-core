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

package io.zyient.base.core.messaging.aws;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.connections.aws.AwsSQSConsumerConnection;
import io.zyient.base.core.connections.settings.aws.AwsSQSConnectionSettings;
import io.zyient.base.core.messaging.MessageObject;
import io.zyient.base.core.messaging.MessageReceiver;
import io.zyient.base.core.messaging.MessagingError;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.Offset;
import io.zyient.base.core.state.OffsetState;
import io.zyient.base.core.state.StateManagerError;
import lombok.NonNull;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class BaseSQSConsumer<M> extends MessageReceiver<String, M> {
    private Queue<MessageObject<String, M>> cache = null;
    private final Map<String, AwsSQSOffsetData> offsetMap = new HashMap<>();
    private AwsSQSConsumerConnection consumer;
    private AwsSQSStateManager stateManager;
    private AwsSQSConsumerState state;
    private TimeUnitValue ackTimeout = new TimeUnitValue(2 * 60 * 1000, TimeUnit.MILLISECONDS);
    private String queueUrl;

    public BaseSQSConsumer<M> withAckTimeout(@NonNull TimeUnitValue value) {
        ackTimeout = value;
        return this;
    }

    @Override
    public void ack(@NonNull String message, boolean commit) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        if (offsetMap.containsKey(message)) {
            synchronized (offsetMap) {
                if (commit) {
                    AwsSQSOffsetData v = offsetMap.remove(message);
                    if (v != null) {
                        delete(v);
                    }
                } else {
                    AwsSQSOffsetData v = offsetMap.get(message);
                    if (v != null) {
                        v.acked(true);
                    }
                }
            }
        }
    }

    private void delete(AwsSQSOffsetData data) throws MessagingError {
        AwsSQSOffsetValue lastIndex = state.getOffset().getOffsetCommitted();
        try {
            DeleteMessageRequest request = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(data.receipt())
                    .build();
            DeleteMessageResponse response = consumer.client().deleteMessage(request);
            if (data.index().getIndex() > lastIndex.getIndex()) {
                lastIndex.setIndex(data.index().getIndex());
                updateCommitState(lastIndex);
            }
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    @Override
    public int commit() throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        int count = 0;
        AwsSQSOffsetValue lastIndex = state.getOffset().getOffsetCommitted();
        try {
            synchronized (offsetMap) {
                if (!offsetMap.isEmpty()) {
                    List<String> acked = new ArrayList<>();
                    List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
                    for (String key : offsetMap.keySet()) {
                        AwsSQSOffsetData v = offsetMap.get(key);
                        if (v != null && v.acked()) {
                            acked.add(key);
                            DeleteMessageBatchRequestEntry entry = DeleteMessageBatchRequestEntry.builder()
                                    .id(v.key())
                                    .receiptHandle(v.receipt())
                                    .build();
                            entries.add(entry);
                            if (v.index().getIndex() > lastIndex.getIndex()) {
                                lastIndex.setIndex(v.index().getIndex());
                            }
                            count++;
                        }
                    }
                    if (!entries.isEmpty()) {
                        DeleteMessageBatchRequest request = DeleteMessageBatchRequest.builder()
                                .entries(entries)
                                .queueUrl(queueUrl)
                                .build();
                        consumer.client().deleteMessageBatch(request);
                        updateCommitState(lastIndex);
                    }
                    if (!acked.isEmpty()) {
                        for (String key : acked) {
                            offsetMap.remove(key);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
        return count;
    }

    @Override
    public MessageReceiver<String, M> init() throws MessagingError {
        Preconditions.checkState(connection() instanceof AwsSQSConsumerConnection);
        consumer = (AwsSQSConsumerConnection) connection();
        AwsSQSConnectionSettings settings = (AwsSQSConnectionSettings) consumer.settings();

        cache = new ArrayBlockingQueue<>(batchSize());
        try {
            if (!consumer.isConnected()) {
                consumer.connect();
            }
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(settings.getQueue())
                    .build();
            queueUrl = consumer.getClient().getQueueUrl(getQueueRequest).queueUrl();
            if (stateful()) {
                Preconditions.checkState(offsetStateManager() instanceof AwsSQSStateManager);
                stateManager = (AwsSQSStateManager) offsetStateManager();
                state = stateManager.get(consumer.name());
                if (state == null) {
                    state = stateManager.create(consumer.name(), consumer.settings().getQueue());
                }
                AwsSQSOffset offset = state.getOffset();
                if (offset.getOffsetCommitted().getIndex() > 0) {
                    seek(offset.getOffsetCommitted(), true);
                } else {
                    AwsSQSOffsetValue v = new AwsSQSOffsetValue();
                    v.setIndex(0);
                    seek(v, false);
                }
                if (offset.getOffsetCommitted().compareTo(offset.getOffsetRead()) != 0) {
                    DefaultLogger.warn(
                            String.format("[topic=%s] Read offset ahead of committed, potential resends.",
                                    consumer.name()));
                    offset.setOffsetRead(offset.getOffsetCommitted());
                    stateManager.update(state);
                }
            }
            offsetMap.clear();
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new MessagingError(ex);
        }
    }

    private void updateReadState(AwsSQSOffsetValue offset) throws StateManagerError {
        if (!stateful()) return;
        state.getOffset().setOffsetRead(offset);
        state = stateManager.update(state);
    }

    private void updateCommitState(AwsSQSOffsetValue offset) throws Exception {
        if (!stateful()) return;
        if (offset.getIndex() > state.getOffset().getOffsetRead().getIndex()) {
            throw new Exception(
                    String.format("[topic=%s] Offsets out of sync. [read=%d][committing=%d]",
                            consumer.name(), state.getOffset().getOffsetRead().getIndex(), offset.getIndex()));
        }
        state.getOffset().setOffsetCommitted(offset);
        state = stateManager.update(state);
    }

    private void seek(AwsSQSOffsetValue offset,
                      boolean next) throws Exception {
        String json = JSONUtils.asString(offset, offset.getClass());
        throw new MessagingError(String.format("Seek not supported. [seek offset=%s]", json));
    }

    @Override
    public MessageObject<String, M> receive(long timeout) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        if (cache.isEmpty()) {
            List<MessageObject<String, M>> batch = nextBatch(timeout);
            if (batch != null) {
                cache.addAll(batch);
            }
        }
        if (!cache.isEmpty()) {
            return cache.poll();
        }
        return null;
    }

    @Override
    public List<MessageObject<String, M>> nextBatch(long timeout) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        AwsSQSOffsetValue lastIndex = state.getOffset().getOffsetRead();
        try {
            int t = (int) TimeUnit.MILLISECONDS.toSeconds(timeout);
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .visibilityTimeout(ackTimeout.normalized().intValue())
                    .waitTimeSeconds(t)
                    .maxNumberOfMessages(batchSize())
                    .build();
            List<Message> records = consumer.getClient().receiveMessage(receiveRequest).messages();
            synchronized (offsetMap) {
                if (records != null && !records.isEmpty()) {
                    List<MessageObject<String, M>> messages = new ArrayList<>(records.size());
                    long sequence = lastIndex.getIndex();
                    for (Message message : records) {
                        try {
                            SQSMessage<M> sqsM = parse(message);
                            long next = getMessageSequence(message);
                            if (next > sequence) {
                                sequence = next;
                            }

                            AwsSQSOffsetValue ov = new AwsSQSOffsetValue(next);
                            offsetMap.put(sqsM.id(),
                                    new AwsSQSOffsetData(sqsM.sqsMessageId(), message.receiptHandle(), ov));
                            messages.add(sqsM);
                        } catch (Exception ex) {
                            DefaultLogger.error(String.format("Failed to parse message. [ID=%s]", message.messageId()));
                        }
                    }
                    if (sequence > lastIndex.getIndex()) {
                        lastIndex.setIndex(sequence);
                        updateReadState(lastIndex);
                    }

                    if (!messages.isEmpty()) {
                        return messages;
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    private SQSMessage<M> parse(Message message) throws Exception {
        SQSMessage<M> sqsm = new SQSMessage<>();
        sqsm.sqsMessageId(message.messageId());
        final Map<String, MessageAttributeValue> attributes = message.messageAttributes();
        sqsm.id(getAttributeValue(MessageObject.HEADER_MESSAGE_ID, attributes, false));
        sqsm.correlationId(getAttributeValue(MessageObject.HEADER_CORRELATION_ID, attributes, true));
        String value = getAttributeValue(MessageObject.HEADER_MESSAGE_MODE, attributes, false);
        sqsm.mode(MessageObject.MessageMode.valueOf(value));
        sqsm.key(getAttributeValue(SQSMessage.HEADER_MESSAGE_KEY, attributes, false));
        value = getAttributeValue(SQSMessage.HEADER_MESSAGE_TIMESTAMP, attributes, false);
        Preconditions.checkNotNull(value);
        sqsm.timestamp(Long.parseLong(value));
        M m = deserialize(message.body());
        sqsm.value(m);

        return sqsm;
    }

    private long getMessageSequence(Message message) {
        final Map<MessageSystemAttributeName, String> attrs = message.attributes();
        String seq = attrs.get(MessageSystemAttributeName.SEQUENCE_NUMBER);
        return Long.parseLong(seq);
    }

    private String getAttributeValue(String name,
                                     Map<String, MessageAttributeValue> attributes,
                                     boolean nullable) throws Exception {
        MessageAttributeValue value = attributes.get(name);
        if (value != null) {
            return value.stringValue();
        } else if (!nullable) {
            throw new MessagingError(String.format("Missing message header. [name=%s]", name));
        }
        return null;
    }

    @Override
    public void ack(@NonNull List<String> messageIds) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        boolean found = false;
        for (String id : messageIds) {
            if (offsetMap.containsKey(id)) {
                offsetMap.get(id).acked(true);
                found = true;
            } else {
                DefaultLogger.warn(String.format("[%s] Message not found. [id=%s]", queueUrl, id));
            }
        }
        if (found) {
            commit();
        }
    }

    @Override
    public OffsetState<?, ?> currentOffset(Context context) throws MessagingError {
        if (!stateful())
            return null;
        return state;
    }

    @Override
    public void seek(@NonNull Offset offset,
                     Context context) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        Preconditions.checkArgument(offset instanceof AwsSQSOffset);
        AwsSQSConsumerState s = (AwsSQSConsumerState) currentOffset(context);
        try {
            AwsSQSOffsetValue o = ((AwsSQSOffset) offset).getOffsetCommitted();
            if (s.getOffset().getOffsetRead().getIndex() < o.getIndex()) {
                o = s.getOffset().getOffsetRead();
                ((AwsSQSOffset) offset).setOffsetCommitted(o);
            }
            seek(o, false);
            updateReadState(o);
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    protected abstract M deserialize(String message) throws MessagingError;

    @Override
    public void close() throws IOException {
        if (state().isAvailable()) {
            state().setState(ProcessorState.EProcessorState.Stopped);
        }
        if (cache != null) {
            cache.clear();
            cache = null;
        }
    }
}
