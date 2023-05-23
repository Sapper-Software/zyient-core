package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.core.state.OffsetStateManagerSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaConsumerOffsetSettings extends OffsetStateManagerSettings {
}
