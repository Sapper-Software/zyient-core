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

package io.zyient.cdc.entity.manager;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SchemaManagerSettings extends Settings {
    public static final String __CONFIG_PATH = "schema";

    public static final int DEFAULT_SCHEMA_CACHE_SIZE = 128;
    public static final long DEFAULT_CACHE_TIMEOUT = 5 * 60 * 1000;

    @Config(name = "name")
    private String name;
    @Config(name = "persistence.handler.class", type = Class.class)
    private Class<? extends SchemaDataHandler> handler;
    @Config(name = "persistence.handler.settings", type = Class.class)
    private Class<? extends SchemaDataHandlerSettings> handlerSettingsClass;
    @Config(name = "schemaCacheSize", required = false, type = Integer.class)
    private int schemaCacheSize = DEFAULT_SCHEMA_CACHE_SIZE;
    @Config(name = "cacheTimeout", required = false, type = Long.class)
    private long cacheTimeout = DEFAULT_CACHE_TIMEOUT;
    private SchemaDataHandlerSettings handlerSettings;
}
