package ai.sapper.cdc.core.messaging.kafka.builders;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.messaging.builders.MessageSenderSettings;
import ai.sapper.cdc.core.messaging.kafka.KafkaPartitioner;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaProducerSettings extends MessageSenderSettings {
    @Config(name = "partitioner", required = false, type = Class.class)
    private Class<? extends KafkaPartitioner<?>> partitioner;
}
