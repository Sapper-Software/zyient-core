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

package io.zyient.base.core.connections.settings.aws;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.connections.MessageConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AwsSQSConnectionSettings extends MessageConnectionSettings {
    @Config(name = "region")
    private String region;
    @Config(name = "timeout.reset", required = false, parser = TimeValueParser.class)
    private TimeUnitValue resetTimeout = new TimeUnitValue(30, TimeUnit.MINUTES);

    public AwsSQSConnectionSettings() {
        super(EConnectionType.sqs);
    }

    public AwsSQSConnectionSettings(@NonNull AwsSQSConnectionSettings settings) {
        super(settings);
        region = settings.region;
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        if (getBatchSize() > 10) {
            setBatchSize(10);
        }
    }
}
