package io.zyient.core.mapping.rules.db;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.kafka.BasicKafkaProducerConnection;
import io.zyient.base.core.errors.Errors;
import io.zyient.core.mapping.rules.RuleConfig;
import io.zyient.core.mapping.rules.RuleEvaluationError;
import io.zyient.core.mapping.rules.RuleValidationError;
import io.zyient.core.messaging.MessageObject;
import io.zyient.core.persistence.env.DataStoreEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

@Getter
@Accessors(fluent = true)
public class DBEventPublisherRule<T, K extends IKey, E extends IEntity<K>> extends DBRule<T, K, E> {

    protected BasicKafkaProducerConnection producer;
    protected String topic;

    @Override
    public void setup(@NonNull RuleConfig cfg) throws ConfigurationException {
        super.setup(cfg);
        Preconditions.checkArgument(cfg instanceof DBEventPublisherConfig);
        DBEventPublisherConfig config = (DBEventPublisherConfig) cfg;
        try {
            DataStoreEnv<?> env = BaseEnv.get(config.getEnv(), DataStoreEnv.class);
            if (env == null) {
                throw new Exception(String.format("Data Store environment not found. [name=%s][type=%s]", config.getEnv(), DataStoreEnv.class.getCanonicalName()));
            }
            producer = env.connectionManager().getConnection(config.getProducer(), BasicKafkaProducerConnection.class);
            if (producer == null) {
                throw new ConfigurationException(String.format("Kafka Producer not found. [name=%s]", config.getProducer()));
            }
            if (!producer.isConnected()) {
                producer.connect();
            }
            topic = config.getTopic();
            if (Strings.isNullOrEmpty(topic)) {
                topic = producer.topic();
            }
        } catch (ConfigurationException ce) {
            throw ce;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    protected Object process(@NonNull T response, List<E> entities) throws RuleValidationError, RuleEvaluationError {
        try {
            if (entities != null && !entities.isEmpty()) {
                E entity = entities.get(0);
                if (entity == null) {
                    String id = UUID.randomUUID().toString();
                    KafkaProducer<String, byte[]> p = producer.producer();
                    List<Header> headers = new ArrayList<>();
                    Header h = new RecordHeader(MessageObject.HEADER_MESSAGE_ID, id.getBytes(StandardCharsets.UTF_8));
                    headers.add(h);
                    h = new RecordHeader(MessageObject.HEADER_CORRELATION_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                    headers.add(h);
                    byte[] data = JSONUtils.asBytes(response);
                    Future<RecordMetadata> result = producer.producer().send(new ProducerRecord<>(topic, null, id, data, headers));
                    RecordMetadata rm = result.get();
                }
                return entity;
            }
            return null;
        } catch (Throwable t) {
            throw new RuleEvaluationError(name(), entityType(), expression(), errorCode(), Errors.getDefault().get(__ERROR_TYPE_RULES, errorCode()).getMessage(), t);
        }
    }
}
