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

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.connections.azure.ServiceBusConsumerConnection;
import io.zyient.base.core.connections.settings.azure.AzureServiceBusConnectionSettings;
import io.zyient.base.core.messaging.MessageObject;
import io.zyient.base.core.messaging.MessageReceiver;
import io.zyient.base.core.messaging.MessagingError;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.Offset;
import io.zyient.base.core.state.OffsetState;
import io.zyient.base.core.state.StateManagerError;
import lombok.NonNull;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class AzureMessageConsumer<M> extends MessageReceiver<String, M> {
    private Queue<MessageObject<String, M>> cache = null;
    private final Map<String, AzureMessageOffsetData> offsetMap = new HashMap<>();
    private ServiceBusConsumerConnection consumer;
    private AzureMessagingStateManager stateManager;
    private AzureMessagingConsumerState state;
    private TimeUnitValue ackTimeout = new TimeUnitValue(2 * 60 * 1000, TimeUnit.MILLISECONDS);
    private int batchSize;

    public AzureMessageConsumer<M> withBatchSize(int batchSize) {
        Preconditions.checkArgument(batchSize > 0);
        this.batchSize = batchSize;
        return this;
    }

    public AzureMessageConsumer<M> withAckTimeout(@NonNull TimeUnitValue value) {
        ackTimeout = value;
        return this;
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
                DefaultLogger.warn(
                        String.format("[%s] Message not found. [id=%s]", consumer.settings().getQueue(), id));
            }
        }
        if (found) {
            commit();
        }
    }

    @Override
    public void ack(@NonNull String message, boolean commit) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        if (offsetMap.containsKey(message)) {
            synchronized (offsetMap) {
                try {
                    if (commit) {
                        AzureMessageOffsetData d = offsetMap.remove(message);
                        consumer.client().complete(d.message());
                        AzureMessageOffsetValue lastIndex = state.getOffset().getOffsetCommitted();
                        if (d.index().compareTo(lastIndex) > 0) {
                            updateCommitState(d.index());
                        }
                    } else {
                        AzureMessageOffsetData d = offsetMap.get(message);
                        d.acked(true);
                    }
                } catch (Exception ex) {
                    throw new MessagingError(ex);
                }
            }
        }
    }

    @Override
    public MessageReceiver<String, M> init() throws MessagingError {
        Preconditions.checkState(connection() instanceof ServiceBusConsumerConnection);
        consumer = (ServiceBusConsumerConnection) connection();
        cache = new ArrayBlockingQueue<>(batchSize());
        try {
            if (!consumer.isConnected()) {
                consumer.connect();
            }
            AzureServiceBusConnectionSettings settings
                    = (AzureServiceBusConnectionSettings) consumer.settings();
            if (stateful()) {
                Preconditions.checkState(offsetStateManager() instanceof AzureMessagingStateManager);
                stateManager = (AzureMessagingStateManager) offsetStateManager();
                state = stateManager.get(consumer.name());
                if (state == null) {
                    state = stateManager.create(consumer.name(), consumer.settings().getQueue());
                }
                AzureMessageOffset offset = state.getOffset();
                //TODO:  seek(offset.getOffsetCommitted(), offset.getOffsetCommitted().getIndex() > 0);
                if (offset.getOffsetCommitted().compareTo(offset.getOffsetRead()) != 0) {
                    DefaultLogger.warn(
                            String.format("[topic=%s] Read offset ahead of committed, potential resends.",
                                    consumer.name()));
                    offset.setOffsetRead(new AzureMessageOffsetValue(offset.getOffsetCommitted()));
                    stateManager.update(state);
                }
            }
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new MessagingError(ex);
        }
    }

    private void seek(AzureMessageOffsetValue offset, boolean next) throws Exception {
        String json = JSONUtils.asString(offset, offset.getClass());
        throw new MessagingError(String.format("Seek not supported. [seek offset=%s]", json));
    }

    @Override
    public int commit() throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        int count = 0;
        AzureMessageOffsetValue lastIndex = state.getOffset().getOffsetCommitted();
        try {
            synchronized (offsetMap) {
                if (!offsetMap.isEmpty()) {
                    List<AzureMessageOffsetData> acked = new ArrayList<>();
                    for (String key : offsetMap.keySet()) {
                        AzureMessageOffsetData d = offsetMap.get(key);
                        if (d.acked()) {
                            acked.add(d);
                            if (d.index().compareTo(lastIndex) > 0) {
                                lastIndex = d.index();
                            }
                        }
                    }
                    if (!acked.isEmpty()) {
                        for (AzureMessageOffsetData message : acked) {
                            consumer.client().complete(message.message());
                            offsetMap.remove(message.key());
                        }
                        updateCommitState(lastIndex);
                    }
                }
                return count;
            }
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    @Override
    public boolean stateful() {
        return true;
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
        AzureMessageOffsetValue lastIndex = state.getOffset().getOffsetRead();
        try {
            IterableStream<ServiceBusReceivedMessage> records =
                    consumer.client().receiveMessages(batchSize, Duration.of(timeout, ChronoUnit.MILLIS));
            synchronized (offsetMap) {
                List<MessageObject<String, M>> messages = new ArrayList<>();
                long sequence = lastIndex.getIndex();
                for (ServiceBusReceivedMessage message : records) {
                    try {
                        AzureMessage<M> m = parse(message);
                        if (m.sequence() > sequence) {
                            sequence = m.sequence();
                        }
                        AzureMessageOffsetValue ov = new AzureMessageOffsetValue(m.sequence());
                        offsetMap.put(m.id(), new AzureMessageOffsetData(m.id(), ov, message));
                        messages.add(m);
                    } catch (Exception ex) {
                        DefaultLogger.error(String.format("Failed to parse message. [ID=%s]", message.getMessageId()));
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
            return null;
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    private void updateReadState(AzureMessageOffsetValue offset) throws StateManagerError {
        if (!stateful()) return;
        state.getOffset().setOffsetRead(offset);
        state = stateManager.update(state);
    }

    private void updateCommitState(AzureMessageOffsetValue offset) throws Exception {
        if (!stateful()) return;
        if (offset.getIndex() > state.getOffset().getOffsetRead().getIndex()) {
            throw new Exception(
                    String.format("[topic=%s] Offsets out of sync. [read=%d][committing=%d]",
                            consumer.name(), state.getOffset().getOffsetRead().getIndex(), offset.getIndex()));
        }
        state.getOffset().setOffsetCommitted(offset);
        state = stateManager.update(state);
    }

    private AzureMessage<M> parse(ServiceBusReceivedMessage message) throws Exception {
        AzureMessage<M> m = new AzureMessage<>();
        m.id(message.getMessageId());
        m.correlationId(message.getCorrelationId());
        m.session(message.getSessionId());
        m.key(message.getSubject());
        m.sequence(message.getSequenceNumber());
        Object mode = message.getApplicationProperties().get(MessageObject.HEADER_MESSAGE_MODE);
        Preconditions.checkArgument(mode instanceof String);
        m.mode(MessageObject.MessageMode.valueOf((String) mode));
        return m;
    }

    @Override
    public void seek(@NonNull Offset offset, Context context) throws MessagingError {

    }

    @Override
    public OffsetState<?, ?> currentOffset(Context context) throws MessagingError {
        if (!stateful())
            return null;
        return state;
    }

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

    @Override
    public String getMessageId(@NonNull MessageObject<String, M> message) {
        return message.id();
    }

    protected abstract M deserialize(byte[] message) throws MessagingError;
}
