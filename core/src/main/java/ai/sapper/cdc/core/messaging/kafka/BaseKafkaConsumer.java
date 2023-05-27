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
    private static final long DEFAULT_RECEIVE_TIMEOUT = 30000; // 30 secs default timeout.
    private Queue<MessageObject<String, M>> cache = null;
    private final Map<String, KafkaOffsetData> offsetMap = new HashMap<>();
    private KafkaStateManager stateManager;
    private BasicKafkaConsumerConnection consumer = null;
    private String topic;
    private KafkaConsumerState state;
    private long defaultReceiveTimeout = DEFAULT_RECEIVE_TIMEOUT;
    private int partition;

    public BaseKafkaConsumer<M> withReceiveTimeout(long receiveTimeout) {
        Preconditions.checkArgument(receiveTimeout > 0);
        defaultReceiveTimeout = receiveTimeout;

        return this;
    }

    private void seek(TopicPartition partition, long offset) throws Exception {
        if (offset > 0) {
            consumer.consumer().seek(partition, offset);
        } else {
            consumer.consumer().seekToBeginning(Collections.singletonList(partition));
        }
        updateReadState(offset);
    }

    @Override
    public void ack(@NonNull String messageId) throws MessagingError {
        Preconditions.checkState(state().isRunning());
        try {
            if (offsetMap.containsKey(messageId)) {
                Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                        new HashMap<>();
                KafkaOffsetData od = offsetMap.get(messageId);
                currentOffsets.put(od.partition(), od.offset());
                consumer.consumer().commitSync(currentOffsets);
                updateCommitState(od.offset().offset());
            } else {
                throw new MessagingError(String.format("No record offset found for key. [key=%s]", messageId));
            }
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }


    @Override
    public void ack(@NonNull List<String> messageIds) throws MessagingError {
        Preconditions.checkState(state().isRunning());
        Preconditions.checkArgument(!messageIds.isEmpty());
        try {
            Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                    new HashMap<>();
            Map<Integer, Long> offsets = new HashMap<>();
            for (String messageId : messageIds) {
                if (offsetMap.containsKey(messageId)) {
                    KafkaOffsetData od = offsetMap.get(messageId);
                    long currentOffset = offsets.get(od.partition().partition());
                    if (od.offset().offset() > currentOffset) {
                        currentOffsets.put(od.partition(), od.offset());
                        offsets.put(od.partition().partition(), od.offset().offset());
                    }
                } else {
                    throw new MessagingError(String.format("No record offset found for key. [key=%s]", messageId));
                }
            }
            consumer.consumer().commitSync(currentOffsets);
            for (int partition : offsets.keySet()) {
                updateCommitState(offsets.get(partition));
            }
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
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
            if (offset.getOffsetCommitted() > 0) {
                seek(partition, offset.getOffsetCommitted() + 1);
            } else {
                seek(partition, offset.getOffsetCommitted());
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
    public MessageObject<String, M> receive() throws MessagingError {
        return receive(defaultReceiveTimeout);
    }

    @Override
    public MessageObject<String, M> receive(long timeout) throws MessagingError {
        Preconditions.checkState(state().isRunning());
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
    public List<MessageObject<String, M>> nextBatch() throws MessagingError {
        return nextBatch(defaultReceiveTimeout);
    }

    @Override
    public List<MessageObject<String, M>> nextBatch(long timeout) throws MessagingError {
        Preconditions.checkState(state().isRunning());
        try {
            ConsumerRecords<String, byte[]> records = consumer.consumer().poll(Duration.ofMillis(timeout));
            if (records != null && records.count() > 0) {
                List<MessageObject<String, M>> array = new ArrayList<>(records.count());
                Map<Integer, Long> offsets = new HashMap<>();
                for (ConsumerRecord<String, byte[]> record : records) {
                    M cd = deserialize(record.value());
                    KafkaMessage<String, M> response = new KafkaMessage<>(record, cd);

                    if (auditLogger() != null) {
                        auditLogger().audit(getClass(), System.currentTimeMillis(), response.value());
                    }
                    array.add(response);
                    offsetMap.put(response.id(), new KafkaOffsetData(record.key(), record));
                    offsets.put(record.partition(), record.offset());
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

    private void updateReadState(long offset) throws Exception {
        if (!stateful()) return;
        state.getOffset().setOffsetRead(offset);
        state = stateManager.update(state);
    }

    private void updateCommitState(long offset) throws Exception {
        if (!stateful()) return;
        if (offset > state.getOffset().getOffsetRead()) {
            throw new Exception(
                    String.format("[topic=%s][partition=%d] Offsets out of sync. [read=%d][committing=%d]",
                            topic, partition, state.getOffset().getOffsetRead(), offset));
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
            long o = ((KafkaOffset) offset).getOffsetCommitted();
            if (s.getOffset().getOffsetRead() <= o) {
                o = s.getOffset().getOffsetRead();
            }
            seek(partition, o);
            updateReadState(o);
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    protected abstract M deserialize(byte[] message) throws MessagingError;

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.clear();
            cache = null;
        }
        if (state().isRunning()) {
            state().setState(ProcessorState.EProcessorState.Stopped);
        }
    }
}
