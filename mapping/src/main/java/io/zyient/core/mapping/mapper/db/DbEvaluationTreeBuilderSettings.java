/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.core.mapping.mapper.db;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.decisions.ConditionParser;
import io.zyient.core.mapping.decisions.MappingConditionParser;
import io.zyient.core.mapping.readers.impl.db.QueryBuilder;
import io.zyient.core.persistence.AbstractDataStore;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class DbEvaluationTreeBuilderSettings extends Settings {
    @Config(name = "dataStore.name")
    private String dataStore;
    @Config(name = "dataStore.type", type = Class.class)
    private Class<? extends AbstractDataStore<?>> dataStoreType;
    @Config(name = "condition.class", type = Class.class)
    private Class<? extends DBConditionDef> conditionType;
    @Config(name = "condition.parser", required = false, type = Class.class)
    private Class<? extends ConditionParser<?>> parser = MappingConditionParser.class;
    @Config(name = "filter.query")
    private String query;
    @Config(name = "filter.condition", required = false)
    private String condition;
    @Config(name = "filter.builder", type = Class.class)
    private Class<? extends QueryBuilder> builder;
}
