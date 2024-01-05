package io.zyient.core.mapping.readers.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.core.mapping.readers.impl.db.DbInputReader;
import io.zyient.core.mapping.readers.impl.db.QueryBuilder;
import io.zyient.core.persistence.AbstractDataStore;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DbReaderSettings extends ReaderSettings {
    @Config(name = "env")
    private String env;
    @Config(name = "reader", type = Class.class)
    private Class<? extends DbInputReader<?,?>> readerType;
    @Config(name = "dataStore.name")
    private String dataStore;
    @Config(name = "dataStore.type", type = Class.class)
    private Class<? extends AbstractDataStore<?>> dataStoreType;
    @Config(name = "type.key", type = Class.class)
    private Class<? extends IKey> keyType;
    @Config(name = "type.entity", type = Class.class)
    private Class<? extends IEntity<?>> entityType;
    @Config(name = "filter.query")
    private String query;
    @Config(name = "filter.condition", required = false)
    private String condition;
    @Config(name = "filter.builder", type = Class.class)
    private Class<? extends QueryBuilder> builder;
}
