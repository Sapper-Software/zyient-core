package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.common.model.Context;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

public class KafkaContext extends Context {
    public static final String KEY_TOPIC = "topic";
    public static final String KEY_PARTITION = "partition";

    public KafkaContext() {
    }

    public KafkaContext(@NonNull String topic, int partition) {
        setTopic(topic);
        setPartition(partition);
    }

    public KafkaContext setTopic(@NonNull String topic) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        put(KEY_TOPIC, topic);
        return this;
    }

    public KafkaContext setPartition(int partition) {
        Preconditions.checkArgument(partition >= 0);
        put(KEY_PARTITION, partition);
        return this;
    }

    public String getTopic() {
        Object v = get(KEY_TOPIC);
        if (v != null) {
            return (String) v;
        }
        return null;
    }

    public int getPartition() {
        Object v = get(KEY_PARTITION);
        if (v != null) {
            return (Integer) v;
        }
        return -1;
    }
}
