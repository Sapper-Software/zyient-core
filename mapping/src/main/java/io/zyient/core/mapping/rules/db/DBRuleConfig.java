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

package io.zyient.core.mapping.rules.db;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.core.mapping.rules.BaseRuleConfig;
import io.zyient.core.mapping.rules.Rule;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DBRuleConfig extends BaseRuleConfig {
    @Config(name = "dataStore")
    private String dataStore;
    @Config(name = "keyType", type = Class.class)
    private Class<? extends IKey> keyType;
    @Config(name = "entityType", type = Class.class)
    private Class<? extends IEntity<?>> entityType;
    @Config(name = "fieldMappings", required = false, custom = FieldMappingReader.class)
    private Map<String, String> fieldMappings;
    @Config(name = "handler", required = false, type = Class.class)
    private Class<? extends DBRuleHandler<?, ?, ?>> handler;
    @Config(name = "cache.enable", required = false, type = Boolean.class)
    private boolean useCache = true;
    @Config(name = "cache.size", required = false, type = Integer.class)
    private int cacheSize = 128;
    @Config(name = "cache.timeout", required = false, parser = TimeValueParser.class)
    private TimeUnitValue cacheTimeout = new TimeUnitValue(60, TimeUnit.MINUTES);

    @Override
    public <E> Rule<E> createInstance(@NonNull Class<? extends E> type) throws Exception {
        return createInstance(type, keyType, entityType);
    }

    private <E, TK extends IKey, TE extends IEntity<TK>> Rule<E> createInstance(Class<? extends E> type,
                                                                                Class<? extends IKey> keyType,
                                                                                Class<? extends IEntity<?>> entityType) throws Exception {
        return new DBReferenceRule<E, TK, TE>();
    }
}
