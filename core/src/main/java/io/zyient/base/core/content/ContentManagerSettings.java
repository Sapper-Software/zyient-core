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

package io.zyient.base.core.content;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.stores.AbstractDataStore;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ContentManagerSettings extends Settings {
    @Config(name = "name")
    private String name;
    @Config(name = "metadata.save", required = false, type = Boolean.class)
    private boolean saveMetadata = false;
    @Config(name = "metadata.dataStore", required = false)
    private String metadataStore;
    @Config(name = "metadata.dataStoreType", required = false, type = Class.class)
    private Class<? extends AbstractDataStore<?>> metadataStoreType;
    @Config(name = "baseDir")
    private String baseDir;
    @Config(name = "cleanOnExit", required = false, type = Boolean.class)
    private boolean cleanOnExit = true;

    public void validate() throws ConfigurationException {
        if (saveMetadata) {
            if (Strings.isNullOrEmpty(metadataStore)) {
                throw new ConfigurationException(String.format("[%s] Metadata store not specified...", name));
            }
            if (metadataStoreType == null) {
                throw new ConfigurationException(String.format("[%s] Metadata store type not specified...", name));
            }
        }
    }
}
