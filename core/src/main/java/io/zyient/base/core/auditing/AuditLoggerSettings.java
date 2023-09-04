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

package io.zyient.base.core.auditing;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.ClassSetParser;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "audit-logger")
public class AuditLoggerSettings extends Settings {
    public static final String __CONFIG_PATH = "audit.logger";
    public static final int MAX_CACHE_SIZE = 32;

    @Config(name = "name")
    private String name;
    @Config(name = "serializer", type = Class.class)
    private Class<? extends IAuditSerDe> serializerClass;
    @Config(name = "default", required = false, type = Boolean.class)
    private boolean defaultLogger = false;
    @Config(name = "dataStore.name")
    private String dataStoreName;
    @Config(name = "dataStore.class", type = Class.class)
    private Class<? extends AbstractDataStore<?>> dataStoreType;
    @Config(name = "classes", parser = ClassSetParser.class)
    private Set<Class<?>> classes;
    @Config(name = "userCache", required = false, type = Boolean.class)
    private boolean useCache = false;
    @Config(name = "maxCacheSize", required = false, type = Integer.class)
    private int maxCacheSize = MAX_CACHE_SIZE;
}
