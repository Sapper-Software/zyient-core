package io.zyient.core.messaging.kafka;

import io.zyient.base.common.messaging.MessagingError;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.messaging.MessageReceiver;
import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Set;

public abstract class AutoAssignBaseKafkaConsumer<M> extends AbstractBaseKafkaConsumer<M> {
    @Override
    public MessageReceiver<String, M> init() throws MessagingError {
        return super.init();
    }

    @Override
    protected Set<TopicPartition> getAssignedTopicPartitions() {
        return new HashSet<>();
    }

    @Override
    protected void beforeNextBatch() throws Exception {
        if(!state().isRunning()){
            Thread.sleep(5000);
            Set<TopicPartition> partitions = consumer.consumer().assignment();
            if (partitions == null || partitions.isEmpty()) {
                throw new MessagingError(String.format("No assigned partitions found. [name=%s][topic=%s]",
                        consumer.name(), topic));
            }
            initializeState(partitions);
            state().setState(ProcessorState.EProcessorState.Running);
        }
    }
}
