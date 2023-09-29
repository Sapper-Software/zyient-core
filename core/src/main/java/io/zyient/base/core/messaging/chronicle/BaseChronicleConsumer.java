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

package io.zyient.base.core.messaging.chronicle;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.connections.chronicle.ChronicleConsumerConnection;
import io.zyient.base.core.messaging.InvalidMessageError;
import io.zyient.base.core.messaging.MessageObject;
import io.zyient.base.core.messaging.MessageReceiver;
import io.zyient.base.core.messaging.MessagingError;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.Offset;
import io.zyient.base.core.state.OffsetState;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class BaseChronicleConsumer<M> extends MessageReceiver<String, M> {
    private Queue<MessageObject<String, M>> cache = null;
    private final Map<String, ChronicleOffsetData> offsetMap = new HashMap<>();
    private ChronicleConsumerConnection consumer;
    private ChronicleStateManager stateManager;
    private ChronicleConsumerState state;
    private String id = UUID.randomUUID().toString();
    private ExcerptTailer tailer;

    public String queue() {
        return consumer.settings().getQueue();
    }

    @Override
    public MessageReceiver<String, M> init() throws MessagingError {
        Preconditions.checkState(connection() instanceof ChronicleConsumerConnection);
        consumer = (ChronicleConsumerConnection) connection();
        cache = new ArrayBlockingQueue<>(batchSize());
        try {
            if (!consumer.isConnected()) {
                consumer.connect();
            }
            tailer = consumer.get(id);
            if (stateful()) {
                Preconditions.checkArgument(offsetStateManager() instanceof ChronicleStateManager);
                stateManager = (ChronicleStateManager) offsetStateManager();
                state = stateManager.get(consumer.name());
                if (state == null) {
                    state = stateManager.create(consumer.name(), consumer.settings().getQueue());
                }
                ChronicleOffset offset = state.getOffset();
                seek(offset.getOffsetCommitted(), offset.getOffsetCommitted().getIndex() > 0);
                if (offset.getOffsetCommitted().compareTo(offset.getOffsetRead()) != 0) {
                    DefaultLogger.warn(
                            String.format("[topic=%s] Read offset ahead of committed, potential resends.",
                                    consumer.name()));
                    offset.setOffsetRead(new ChronicleOffsetValue(offset.getOffsetCommitted()));
                    stateManager.update(state);
                }
            }
            offsetMap.clear();
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            throw new MessagingError(ex);
        }
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
        long stime = System.currentTimeMillis();
        long remaining = timeout;
        ChronicleOffsetValue lastIndex = state.getOffset().getOffsetRead();
        List<MessageObject<String, M>> messages = new ArrayList<>(batchSize());
        try {
            synchronized (offsetMap) {
                while (remaining > 0) {
                    boolean read = false;
                    try (DocumentContext dc = tailer.readingDocument(true)) {
                        if (dc.isPresent()) {
                            if (!dc.isData()) {
                                continue;
                            }
                            ReadResponse<M> response = parse(dc);
                            response.index = new ChronicleOffsetValue(tailer.cycle(), tailer.index());
                            if (response.index.compareTo(lastIndex) > 0) {
                                lastIndex = response.index;
                            }
                            if (response.error != null) {
                                if (response.error instanceof InvalidMessageError) {
                                    DefaultLogger.error("Error reading message.", response.error);
                                } else {
                                    throw response.error;
                                }
                            } else if (response.message != null) {
                                response.message.index(response.index);
                                messages.add(response.message);
                                offsetMap.put(response.message.id(),
                                        new ChronicleOffsetData(response.message.key(), response.index));
                                read = true;
                            }
                        }
                    }
                    if (!read) {
                        long si = remaining / 10;
                        if (si > 0) {
                            RunUtils.sleep(si);
                        }
                        remaining -= System.currentTimeMillis() - stime;
                        continue;
                    }
                    if (messages.size() >= batchSize()) {
                        break;
                    }
                }
            }
            if (lastIndex.compareTo(state.getOffset().getOffsetRead()) > 0) {
                updateReadState(lastIndex);
            }
            if (!messages.isEmpty()) {
                return messages;
            }

            return null;
        } catch (Throwable ex) {
            DefaultLogger.stacktrace(ex);
            throw new MessagingError(ex);
        }
    }

    private ReadResponse<M> parse(DocumentContext context) throws Exception {

        final ReadResponse<M> response = new ReadResponse<>();
        Wire w = context.wire();
        if (w != null) {
            MessageEnvelop envelop = w.read().object(MessageEnvelop.class);
            if (envelop == null) {
                throw new MessagingError(String.format("Failed to read data. [queue=%s][index=%d]",
                        consumer.settings().getQueue(), response.index.getIndex()));
            }
            M data = deserialize(envelop.data());
            final BaseChronicleMessage<M> message = new BaseChronicleMessage<>(envelop);
            message.value(data);
            response.message = message;
        }
        return response;
    }


    @Override
    public void ack(@NonNull String messageId, boolean commit) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        try {
            synchronized (offsetMap) {
                if (offsetMap.containsKey(messageId)) {
                    ChronicleOffsetData od = offsetMap.get(messageId);
                    od.acked(true);
                    if (commit) {
                        updateCommitState(od.index());
                        offsetMap.remove(messageId);
                    }
                } else {
                    throw new MessagingError(String.format("No record offset found for key. [key=%s]", messageId));
                }
            }
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    @Override
    public void ack(@NonNull List<String> messageIds) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        Preconditions.checkArgument(!messageIds.isEmpty());
        try {
            synchronized (offsetMap) {
                ChronicleOffsetValue currentOffset = new ChronicleOffsetValue(0, -1L);
                for (String messageId : messageIds) {
                    if (offsetMap.containsKey(messageId)) {
                        ChronicleOffsetData od = offsetMap.get(messageId);
                        currentOffset = (od.index().compareTo(currentOffset) > 0 ? od.index() : currentOffset);
                    } else {
                        throw new MessagingError(String.format("No record offset found for key. [key=%s]", messageId));
                    }
                    offsetMap.remove(messageId);
                }
                if (currentOffset.compareTo(state.getOffset().getOffsetCommitted()) > 0) {
                    updateCommitState(currentOffset);
                }
            }
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    @Override
    public int commit() throws MessagingError {
        int count = 0;
        synchronized (offsetMap) {
            List<String> messageIds = new ArrayList<>();
            for (String id : offsetMap.keySet()) {
                ChronicleOffsetData od = offsetMap.get(id);
                if (od.acked()) {
                    messageIds.add(id);
                }
            }
            if (!messageIds.isEmpty()) {
                ack(messageIds);
                count = messageIds.size();
            }
        }
        return count;
    }

    @Override
    public OffsetState<?, ?> currentOffset(Context context) throws MessagingError {
        if (!stateful())
            return null;
        return state;
    }

    @Override
    public void seek(@NonNull Offset offset, Context context) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        Preconditions.checkArgument(offset instanceof ChronicleOffset);
        ChronicleConsumerState s = (ChronicleConsumerState) currentOffset(context);
        try {
            ChronicleOffsetValue o = ((ChronicleOffset) offset).getOffsetCommitted();
            if (s.getOffset().getOffsetRead().getIndex() < o.getIndex()) {
                o = s.getOffset().getOffsetRead();
                ((ChronicleOffset) offset).setOffsetCommitted(o);
            }
            seek(o, false);
            updateReadState(o);
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    private void seek(ChronicleOffsetValue offset, boolean next) throws Exception {
        if (offset.getIndex() > 0) {
            if (!tailer.moveToCycle(offset.getCycle())) {
                throw new Exception(String.format("Failed to move to cycle. [queue=%s][cycle=%d]",
                        consumer.name(), offset.getCycle()));
            }
            if (!tailer.moveToIndex(offset.getIndex())) {
                throw new Exception(
                        String.format("Failed to move to offset. [queue=%s][offset=%d]",
                                consumer.name(), offset.getIndex()));
            }
            if (next) {
                long nextIndex = offset.getIndex() + 1;
                if (!tailer.moveToIndex(nextIndex)) {
                    ChronicleQueue queue = tailer.queue();
                    nextIndex = queue
                            .rollCycle()
                            .toIndex(offset.getCycle() + 1, 0);
                    if (!tailer.moveToIndex(nextIndex)) {
                        DefaultLogger.warn(String.format("[queue=%s] At cycle end.", consumer.name()));
                    }
                }
            }
        } else {
            tailer.toStart();
        }
    }

    private void updateReadState(ChronicleOffsetValue offset) throws Exception {
        if (!stateful()) return;
        state.getOffset().setOffsetRead(offset);
        state = stateManager.update(state);
    }

    private void updateCommitState(ChronicleOffsetValue offset) throws Exception {
        if (!stateful()) return;
        if (offset.getIndex() > state.getOffset().getOffsetRead().getIndex()) {
            throw new Exception(
                    String.format("[topic=%s] Offsets out of sync. [read=%d][committing=%d]",
                            consumer.name(), state.getOffset().getOffsetRead().getIndex(), offset.getIndex()));
        }
        state.getOffset().setOffsetCommitted(offset);
        state = stateManager.update(state);
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
        if (tailer != null) {
            consumer.release(id);
            tailer = null;
        }
    }

    protected abstract M deserialize(byte[] message) throws MessagingError;

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ReadResponse<M> {
        private BaseChronicleMessage<M> message;
        private Throwable error;
        private ChronicleOffsetValue index = new ChronicleOffsetValue();

        public ReadResponse(@NonNull BaseChronicleMessage<M> message) {
            this.message = message;
            error = null;
        }

        public ReadResponse() {
            message = null;
            error = null;
        }
    }
}
