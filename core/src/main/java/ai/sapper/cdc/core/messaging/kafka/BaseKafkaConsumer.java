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

package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.kafka.BasicKafkaConsumerConnection;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageReceiver;
import ai.sapper.cdc.core.messaging.MessagingError;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.core.state.Offset;
import ai.sapper.cdc.core.state.OffsetState;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class BaseKafkaConsumer<M> extends MessageReceiver<String, M> {
    private Queue<MessageObject<String, M>> cache = null;
    private final Map<String, KafkaOffsetData> offsetMap = new HashMap<>();
    private KafkaStateManager stateManager;
    private BasicKafkaConsumerConnection consumer = null;
    private String topic;
    private KafkaConsumerState state;
    private int partition;

    private void seek(TopicPartition partition, long offset) throws Exception {
        if (offset > 0) {
            consumer.consumer().seek(partition, offset);
        } else {
            consumer.consumer().seekToBeginning(Collections.singletonList(partition));
        }
    }

    @Override
    public void ack(@NonNull String messageId, boolean commit) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        try {
            synchronized (offsetMap) {
                if (offsetMap.containsKey(messageId)) {
                    Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                            new HashMap<>();
                    KafkaOffsetData od = offsetMap.get(messageId);
                    od.acked(true);
                    currentOffsets.put(od.partition(), od.offset());
                    if (commit) {
                        consumer.consumer().commitSync(currentOffsets);
                        updateCommitState(new KafkaOffsetValue(od.offset().offset()));
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
                Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                        new HashMap<>();
                Map<Integer, KafkaOffsetValue> offsets = new HashMap<>();
                for (String messageId : messageIds) {
                    if (offsetMap.containsKey(messageId)) {
                        KafkaOffsetData od = offsetMap.get(messageId);
                        KafkaOffsetValue currentOffset = offsets.get(od.partition().partition());
                        if (od.offset().offset() > currentOffset.getValue()) {
                            currentOffsets.put(od.partition(), od.offset());
                            offsets.put(od.partition().partition(), new KafkaOffsetValue(od.offset().offset()));
                        }
                    } else {
                        throw new MessagingError(String.format("No record offset found for key. [key=%s]", messageId));
                    }
                    offsetMap.remove(messageId);
                }
                consumer.consumer().commitSync(currentOffsets);
                for (int partition : offsets.keySet()) {
                    updateCommitState(offsets.get(partition));
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
                KafkaOffsetData od = offsetMap.get(id);
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
    public MessageReceiver<String, M> init() throws MessagingError {
        Preconditions.checkState(connection() instanceof BasicKafkaConsumerConnection);
        consumer = (BasicKafkaConsumerConnection) connection();
        cache = new ArrayBlockingQueue<>(consumer.batchSize());
        topic = consumer.topic();

        try {
            if (!consumer.isConnected()) {
                consumer.connect();
            }
            if (stateful()) {
                Preconditions.checkArgument(offsetStateManager() instanceof KafkaStateManager);
                stateManager = (KafkaStateManager) offsetStateManager();
                initializeStates();
            }
            offsetMap.clear();
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            throw new MessagingError("Error initializing kafka consumer.", ex);
        }
    }

    private void initializeStates() throws Exception {
        Set<TopicPartition> partitions = consumer.consumer().assignment();
        if (partitions == null || partitions.isEmpty()) {
            throw new MessagingError(String.format("No assigned partitions found. [name=%s][topic=%s]",
                    consumer.name(), topic));
        }
        if (partitions.size() > 1) {
            throw new MessagingError(String.format("Multiple assigned partitions found. [name=%s][topic=%s]",
                    consumer.name(), topic));
        }

        for (TopicPartition partition : partitions) {
            state = stateManager.get(topic, partition.partition());
            if (state == null) {
                state = stateManager.create(topic, partition.partition());
            }

            KafkaOffset offset = state.getOffset();
            if (offset.getOffsetCommitted().getValue() > 0) {
                seek(partition, offset.getOffsetCommitted().getValue() + 1);
            } else {
                seek(partition, 0);
            }
            if (offset.getOffsetCommitted() != offset.getOffsetRead()) {
                DefaultLogger.warn(
                        String.format("[topic=%s][partition=%d] Read offset ahead of committed, potential resends.",
                                topic, partition.partition()));
                offset.setOffsetRead(offset.getOffsetCommitted());
                stateManager.update(state);
            }
        }
    }

    private TopicPartition findPartition(int partition) throws MessagingError {
        Set<TopicPartition> partitions = consumer.consumer().assignment();
        if (partitions == null || partitions.isEmpty()) {
            throw new MessagingError(String.format("No assigned partitions found. [name=%s][topic=%s]",
                    consumer.name(), topic));
        }
        for (TopicPartition p : partitions) {
            if (p.partition() == partition) {
                return p;
            }
        }
        return null;
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
        try {
            ConsumerRecords<String, byte[]> records = consumer.consumer().poll(Duration.ofMillis(timeout));
            if (records != null && records.count() > 0) {
                List<MessageObject<String, M>> array = new ArrayList<>(records.count());
                Map<Integer, KafkaOffsetValue> offsets = new HashMap<>();
                for (ConsumerRecord<String, byte[]> record : records) {
                    M cd = deserialize(record.value());
                    KafkaMessage<String, M> response = new KafkaMessage<>(record, cd);

                    array.add(response);
                    offsetMap.put(response.id(), new KafkaOffsetData(record.key(), record));
                    offsets.put(record.partition(), new KafkaOffsetValue(record.offset()));
                }
                for (int partition : offsets.keySet()) {
                    updateReadState(offsets.get(partition));
                }
                return array;
            }
            return null;
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    private void updateReadState(KafkaOffsetValue offset) throws Exception {
        if (!stateful()) return;
        state.getOffset().setOffsetRead(offset);
        state = stateManager.update(state);
    }

    private void updateCommitState(KafkaOffsetValue offset) throws Exception {
        if (!stateful()) return;
        if (offset.compareTo(state.getOffset().getOffsetRead()) > 0) {
            throw new Exception(
                    String.format("[topic=%s][partition=%d] Offsets out of sync. [read=%d][committing=%d]",
                            topic, partition, state.getOffset().getOffsetRead().getValue(), offset.getValue()));
        }
        state.getOffset().setOffsetCommitted(offset);
        state = stateManager.update(state);
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
        Preconditions.checkArgument(context instanceof KafkaContext);
        Preconditions.checkArgument(offset instanceof KafkaOffset);
        KafkaConsumerState s = (KafkaConsumerState) currentOffset(context);
        TopicPartition partition = findPartition(((KafkaContext) context).getPartition());
        if (partition == null) {
            throw new MessagingError(
                    String.format("[%s] Partition not found. [partition=%d]",
                            topic, ((KafkaContext) context).getPartition()));
        }
        try {
            KafkaOffsetValue o = ((KafkaOffset) offset).getOffsetCommitted();
            if (s.getOffset().getOffsetRead().compareTo(o) < 0) {
                o = s.getOffset().getOffsetRead();
                ((KafkaOffset) offset).setOffsetCommitted(o);
            }
            seek(partition, o.getValue());
            updateReadState(o);
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    protected abstract M deserialize(byte[] message) throws MessagingError;

    @Override
    public void close() throws IOException {
        if (state().isAvailable()) {
            state().setState(ProcessorState.EProcessorState.Stopped);
        }
        if (cache != null) {
            cache.clear();
            cache = null;
        }
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }
}
