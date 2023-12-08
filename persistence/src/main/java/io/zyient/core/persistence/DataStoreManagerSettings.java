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

package io.zyient.core.persistence;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DataStoreManagerSettings extends Settings {
    public static final String __CONFIG_PATH = "dataStores";
    public static final String CONFIG_NODE_DATA_STORE = "store";
    public static final String ZK_NODE_SEQUENCE = "sequence";

    @Config(name = "zk.connection", required = false)
    private String zkConnection;
    @Config(name = "zk.path", required = false)
    private String zkPath;
    @Config(name = "zk.autoSave", required = false, type = Boolean.class)
    private boolean autoSave = true;
    @Config(name = "override", required = false, type = Boolean.class)
    private boolean override = false;
}
