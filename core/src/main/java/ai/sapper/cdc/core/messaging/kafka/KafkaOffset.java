package ai.sapper.cdc.core.messaging.kafka;

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
public class KafkaOffset extends Offset {
    private String topic;
    private int partition;
    private long offsetRead = 0;
    private long offsetCommitted = 0;

    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof KafkaOffset);
        return (int) (offsetCommitted - ((KafkaOffset) offset).offsetCommitted);
    }

    @Override
    public String asString() {
        return String.format("%d:%d", offsetRead, offsetCommitted);
    }

    @Override
    public Offset fromString(@NonNull String source) throws Exception {
        String[] parts = source.split(":");
        if (parts.length != 2) {
            throw new Exception(String.format("Invalid offset: %s", source));
        }
        offsetRead = Long.parseLong(parts[0]);
        offsetCommitted = Long.parseLong(parts[1]);
        return this;
    }
}
