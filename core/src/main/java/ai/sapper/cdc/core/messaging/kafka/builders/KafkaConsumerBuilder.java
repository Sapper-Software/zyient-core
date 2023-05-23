package ai.sapper.cdc.core.messaging.kafka.builders;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.kafka.BasicKafkaConsumerConnection;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverBuilder;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverSettings;
import ai.sapper.cdc.core.messaging.kafka.BaseKafkaConsumer;
import ai.sapper.cdc.core.messaging.kafka.KafkaStateManager;
import ai.sapper.cdc.core.state.OffsetStateManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

public class KafkaConsumerBuilder<M> extends MessageReceiverBuilder<String, M> {
    private final Class<? extends BaseKafkaConsumer<M>> type;

    protected KafkaConsumerBuilder(@NonNull Class<? extends BaseKafkaConsumer<M>> type,
                                   @NonNull BaseEnv<?> env,
                                   @NonNull Class<? extends MessageReceiverSettings> settingsType) {
        super(env, settingsType);
        this.type = type;
    }

    @Override
    public BaseKafkaConsumer<M> build(@NonNull MessageReceiverSettings settings) throws Exception {
        Preconditions.checkArgument(settings.getType() == EConnectionType.kafka);
        BasicKafkaConsumerConnection connection = env().connectionManager()
                .getConnection(settings.getConnection(), BasicKafkaConsumerConnection.class);
        if (connection == null) {
            throw new Exception(
                    String.format("Kafka Consumer connection not found. [name=%s]", settings.getConnection()));
        }
        BaseKafkaConsumer<M> consumer = type.getDeclaredConstructor().newInstance();
        consumer.withConnection(connection);
        if (env().auditLogger() != null) {
            consumer.withAuditLogger(env().auditLogger());
        }
        if (!Strings.isNullOrEmpty(settings.getOffsetManager())) {
            OffsetStateManager<?> offsetStateManager = env().stateManager()
                    .getOffsetManager(settings.getOffsetManager(), KafkaStateManager.class);
            if (offsetStateManager == null) {
                throw new Exception(
                        String.format("Kafka State manager not found. [name=%s]", settings.getOffsetManager()));
            }
            consumer.withOffsetStateManager(offsetStateManager);
        }
        if (settings.getReceiverTimeout() > 0) {
            consumer.withReceiveTimeout(settings.getReceiverTimeout());
        }
        return (BaseKafkaConsumer<M>) consumer.init();
    }
}
