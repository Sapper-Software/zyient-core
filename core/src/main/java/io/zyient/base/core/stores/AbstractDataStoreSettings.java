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

package io.zyient.base.core.stores;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.Settings;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "store")
public class AbstractDataStoreSettings extends Settings {
    public static final String CONFIG_SETTING_TYPE = "settingType";
    public static final long SEQUENCE_BLOCK_SIZE = 8;

    private static final int DEFAULT_MAX_RESULTS = 500;
    @Config(name = "class", type = Class.class)
    private Class<? extends AbstractDataStore<?>> dataStoreClass;
    @Config(name = "name")
    private String name;
    @Config(name = "maxResults", required = false, type = Integer.class)
    private int maxResults = DEFAULT_MAX_RESULTS;
    @Config(name = "description", required = false)
    private String description;
    @Config(name = "connection.name")
    private String connectionName;
    @Config(name = "connection.class", type = Class.class)
    private Class<? extends AbstractConnection<?>> connectionType;
    @Config(name = "audited", required = false, type = Boolean.class)
    private boolean audited = false;
    @JsonIgnore
    private EConfigSource source;
    private EDataStoreType type;
    @Config(name = "sequenceBlockSize", required = false, type = Long.class)
    private long sequenceBlockSize = SEQUENCE_BLOCK_SIZE;
}
