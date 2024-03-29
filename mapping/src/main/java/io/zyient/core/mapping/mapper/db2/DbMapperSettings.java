package io.zyient.core.mapping.mapper.db2;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.core.mapping.mapper.db.DBConditionDef;
import io.zyient.core.mapping.readers.impl.db.QueryBuilder;
import io.zyient.core.persistence.AbstractDataStore;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class DbMapperSettings extends Settings {

    @Config(name = "dataStore.name")
    private String dataStore;
    @Config(name = "dataStore.type", type = Class.class)
    private Class<? extends AbstractDataStore<?>> dataStoreType;
    @Config(name = "condition.class", type = Class.class)
    private Class<? extends DBMappingConf> conditionType;
    @Config(name = "filter.mappingQuery")
    private String mappingQuery;
    @Config(name = "filter.filterQuery")
    private String filterQuery;
    @Config(name = "filter.condition", required = false)
    private String condition;
    @Config(name = "filter.builder", type = Class.class)
    private Class<? extends QueryBuilder> builder;
    @Config(name = "sourceAsSource", required = false, type = Boolean.class)
    private Boolean sourceAsSource = true;

}
