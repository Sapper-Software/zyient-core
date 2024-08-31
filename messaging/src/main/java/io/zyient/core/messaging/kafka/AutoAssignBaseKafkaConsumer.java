package io.zyient.core.messaging.kafka;

import io.zyient.base.common.messaging.MessagingError;
import io.zyient.core.messaging.MessageReceiver;

public abstract class AutoAssignBaseKafkaConsumer<M> extends AbstractBaseKafkaConsumer<M> {
    @Override
    public MessageReceiver<String, M> init() throws MessagingError {
        return super.init();
    }

    @Override
    protected void initializeState() throws Exception {

    }
}
