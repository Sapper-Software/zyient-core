package ai.sapper.cdc.core.messaging.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Getter
@Setter
@Accessors(fluent = true)
public class KafkaOffsetData {
    private final String key;
    private final TopicPartition partition;
    private final OffsetAndMetadata offset;

    public KafkaOffsetData(String key, ConsumerRecord<String, byte[]> record) {
        this.key = key;
        this.partition = new TopicPartition(record.topic(), record.partition());
        this.offset = new OffsetAndMetadata(record.offset(), String.format("[Key=%s]", key));
    }
}
