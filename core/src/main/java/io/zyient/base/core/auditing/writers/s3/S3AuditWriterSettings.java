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

package io.zyient.base.core.auditing.writers.s3;

import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.config.units.TimeValueParser;
import io.zyient.base.core.auditing.writers.local.FileAuditWriterSettings;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

@Getter
@Setter
public class S3AuditWriterSettings extends FileAuditWriterSettings {
    @Config(name = "connection")
    private String connection;
    @Config(name = "bucket")
    private String bucket;
    @Config(name = "sync.root")
    private String root;
    @Config(name = "sync.interval", required = false, parser = TimeValueParser.class)
    private TimeUnitValue syncInterval = new TimeUnitValue(5, TimeUnit.MINUTES);
}
