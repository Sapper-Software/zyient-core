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
