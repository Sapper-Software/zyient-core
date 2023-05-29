package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.state.OffsetState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaConsumerState extends OffsetState<Connection.EConnectionState, KafkaOffset> {
    public static final String OFFSET_TYPE = "kafka/consumer";

    private String topic;
    private long partition = 0;
    private long updateTimestamp;

    public KafkaConsumerState() {
        super(Connection.EConnectionState.Error, Connection.EConnectionState.Initialized);
        setType(OFFSET_TYPE);
    }
}
