package io.zyient.core.mapping.rules.db;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.core.mapping.rules.Rule;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DBEventPublisherConfig extends DBRuleConfig {

    @Config(name = "env")
    private String env;
    @Config(name = "topic")
    private String topic;
    @Config(name = "producer")
    private String producer;

    @Override
    public <E> Rule<E> createInstance(@NonNull Class<? extends E> type) throws Exception {
        return this.createInstance(type, this.getKeyType(), this.getEntityType());
    }

    private <E, TK extends IKey, TE extends IEntity<TK>> Rule<E> createInstance(Class<? extends E> type, Class<? extends IKey> keyType, Class<? extends IEntity<?>> entityType) throws Exception {
        return new DBEventPublisherRule<>();
    }
}
