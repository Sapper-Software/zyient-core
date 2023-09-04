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

package io.zyient.base.core.connections.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.connections.EMessageClientMode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.util.concurrent.TimeUnit;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ChronicleSettings extends ConnectionSettings {
    @Config(name = "queue")
    private String queue;
    @Config(name = "baseDir")
    private String baseDir;
    @Config(name = EMessageClientMode.CONFIG_MODE, required = false, type = EMessageClientMode.class)
    private EMessageClientMode mode = EMessageClientMode.Producer;
    @Config(name = "retention", required = false, parser = TimeValueParser.class)
    private TimeUnitValue cleanUpTTL = new TimeUnitValue(5L * 60 * 60 * 1000, TimeUnit.MILLISECONDS); // Default = 5Hrs
    @Config(name = "rollCycle", required = false, type = RollCycles.class)
    private RollCycles rollCycle = RollCycles.FAST_HOURLY;
    @Config(name = "indexSpacing", required = false, type = Integer.class)
    private int indexSpacing = 64;

    public ChronicleSettings() {

    }

    public ChronicleSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof ChronicleSettings);
        queue = ((ChronicleSettings) settings).queue;
        baseDir = ((ChronicleSettings) settings).baseDir;
        cleanUpTTL = ((ChronicleSettings) settings).cleanUpTTL;
        rollCycle = ((ChronicleSettings) settings).rollCycle;
        indexSpacing = ((ChronicleSettings) settings).indexSpacing;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        if (indexSpacing != 16 && indexSpacing != 64) {
            throw new ConfigurationException(
                    String.format("Invalid configuration: Index Spacing value. [value=%d]", indexSpacing));
        }
    }
}
