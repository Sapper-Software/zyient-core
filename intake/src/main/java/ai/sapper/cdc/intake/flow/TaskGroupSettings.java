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

package ai.sapper.cdc.intake.flow;

import ai.sapper.cdc.common.config.*;
import ai.sapper.cdc.common.config.units.TimeUnitValue;
import ai.sapper.cdc.common.config.units.TimeValueParser;
import ai.sapper.cdc.intake.flow.datastore.TaskFlowErrorHandlerSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "taskGroup")
public class TaskGroupSettings extends Settings {
    public static final long DEFAULT_STORE_REFRESH_INTERVAL = 30 * 60 * 1000;

    @Config(name = "name")
    private String name;
    @Config(name = "namespace")
    private String namespace;
    @Config(name = "stores.dynamic", required = false, type = Boolean.class)
    private boolean dynamicStores = false;
    @Config(name = "stores.connection", required = false)
    private String zkConnection;
    @Config(name = "stores.path", required = false)
    private String zkPath;
    @Config(name = "stores.refreshInterval", required = false, parser = TimeValueParser.class)
    private TimeUnitValue storeRefreshInterval = new TimeUnitValue(DEFAULT_STORE_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
    @Config(name = "stores.sources", required = false, parser = StringListParser.class)
    private List<String> dataSourceNames;
    @Config(name = "allowConcurrentPerStore", required = false, type = Boolean.class)
    private boolean allowConcurrentPerStore = true;
    @Config(name = "audited", required = false, type = Boolean.class)
    private boolean audited = false;
    @Config(name = "errorHandlerSettings", required = false, type = Class.class)
    private Class<? extends TaskFlowErrorHandlerSettings> errorHandlerSettingsType;
}
