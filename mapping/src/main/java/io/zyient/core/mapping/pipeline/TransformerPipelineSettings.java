/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.pipeline;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.stores.AbstractDataStore;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class TransformerPipelineSettings extends Settings {
    @Config(name = "mapping")
    private String mapping;
    @Config(name = "keyType", type = Class.class)
    private Class<? extends IKey> keyType;
    @Config(name = "entityType", type = Class.class)
    private Class<? extends IEntity<?>> entityType;
    @Config(name = "terminateOnValidationError", required = false, type = Boolean.class)
    private boolean terminateOnValidationError = true;
    @Config(name = "saveValidationErrors", required = false, type = Boolean.class)
    private boolean saveValidationErrors = false;
    @Config(name = "dataStore.name")
    private String dataStore;
    @Config(name = "dataStore.type", type = Class.class)
    private Class<? extends AbstractDataStore<?>> dataStoreType;
    @Config(name = "dataStore.commitBatchSize", required = false, type = Integer.class)
    private int commitBatchSize = 256;
}
