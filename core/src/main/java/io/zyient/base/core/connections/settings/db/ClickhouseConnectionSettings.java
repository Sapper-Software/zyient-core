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

package io.zyient.base.core.connections.settings.db;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.lists.StringListParser;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "clickhouse")
public class ClickhouseConnectionSettings extends ConnectionSettings {
    public static final String __CONFIG_PATH = "clickhouse";

    @Config(name = "protocol", required = false)
    private String protocol = "http";
    @Config(name = "db.urls", parser = StringListParser.class)
    private List<String> dbUrls;
    @Config(name = "port", required = false, type = Integer.class)
    private int port = -1;
    @Config(name = "user")
    private String username;
    @Config(name = "passKey", required = false)
    private String passKey;
    @Config(name = "db.name")
    private String db;
    @Config(name = "ssl.enable", required = false, type = Boolean.class)
    private boolean useSSL = true;
    @Config(name = "ssl.mode", required = false)
    private String sslMode = "STRICT";
}
