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

package io.zyient.core.caseflow;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.core.caseflow.model.Case;
import io.zyient.core.caseflow.model.CaseDocument;
import io.zyient.core.persistence.AbstractDataStore;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CaseManagerSettings extends Settings {
    public static final String __CONFIG_PATH = "case-manager";

    @Config(name = "case.type", type = Class.class)
    private Class<? extends Case<?, ?, ?>> caseType;
    @Config(name = "dataStore.name")
    private String dataStore;
    @Config(name = "dataStore.type", type = Class.class)
    private Class<? extends AbstractDataStore<?>> dataStoreType;
    @Config(name = "documentType", type = Class.class)
    private Class<? extends CaseDocument<?, ?>> documentType;
    @Config(name = "authorizer", type = Class.class)
    private Class<? extends ActionAuthorization<?>> authorizer;
    @Config(name = "content.collection")
    private String contentCollection;
}
