package ai.sapper.cdc.core.messaging.kafka.builders;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.kafka.BasicKafkaProducerConnection;
import ai.sapper.cdc.core.messaging.MessageReceiver;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverSettings;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageSenderSettings;
import ai.sapper.cdc.core.messaging.kafka.BasicKafkaProducer;
import ai.sapper.cdc.core.messaging.kafka.KafkaPartitioner;
import com.google.common.base.Preconditions;
import lombok.NonNull;

public class KafkaProducerBuilder<M> extends MessageSenderBuilder<String, M> {
    private final Class<? extends BasicKafkaProducer<M>> type;

    protected KafkaProducerBuilder(@NonNull Class<? extends BasicKafkaProducer<M>> type,
                                   @NonNull BaseEnv<?> env,
                                   @NonNull Class<? extends MessageSenderSettings> settingsType) {
        super(env, settingsType);
        this.type = type;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BasicKafkaProducer<M> build(@NonNull MessageSenderSettings settings) throws Exception {
        Preconditions.checkArgument(settings instanceof KafkaProducerSettings);
        BasicKafkaProducerConnection connection = env().connectionManager()
                .getConnection(settings.getConnection(), BasicKafkaProducerConnection.class);
        if (connection == null) {
            throw new Exception(
                    String.format("Kafka Producer connection not found. [name=%s]", settings.getConnection()));
        }
        BasicKafkaProducer<M> producer = type.getDeclaredConstructor().newInstance();
        producer.withConnection(connection);
        if (env().auditLogger() != null) {
            producer.withAuditLogger(env().auditLogger());
        }
        if (((KafkaProducerSettings) settings).getPartitioner() != null) {
            KafkaPartitioner<M> partitioner = (KafkaPartitioner<M>) ((KafkaProducerSettings) settings)
                    .getPartitioner()
                    .getDeclaredConstructor()
                    .newInstance();
        }
        return producer;
    }
}
