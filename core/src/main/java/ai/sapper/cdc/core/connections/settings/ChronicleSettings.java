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

package ai.sapper.cdc.core.connections.settings;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.units.TimeUnitValue;
import ai.sapper.cdc.common.config.units.TimeValueParser;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ChronicleSettings extends ConnectionSettings {
    @Config(name = "queue")
    private String name;
    @Config(name = "path")
    private String path;
    @Config(name = "retention", required = false, parser = TimeValueParser.class)
    private TimeUnitValue cleanUpTTL = new TimeUnitValue(5L * 60 * 60 * 1000, TimeUnit.MILLISECONDS); // Default = 5Hrs

    public ChronicleSettings() {

    }

    public ChronicleSettings(@NonNull ConnectionSettings settings) {
        super(settings);
    }
}
