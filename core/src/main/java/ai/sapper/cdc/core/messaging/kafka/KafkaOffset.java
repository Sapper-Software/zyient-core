package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.core.messaging.ReceiverOffset;
import ai.sapper.cdc.core.state.Offset;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaOffset extends ReceiverOffset {
    private String topic;
    private int partition;

    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof KafkaOffset);
        Preconditions.checkArgument(topic.compareTo(((KafkaOffset) offset).topic) == 0);
        Preconditions.checkArgument(partition == ((KafkaOffset) offset).partition);
        return super.compareTo(offset);
    }
}
