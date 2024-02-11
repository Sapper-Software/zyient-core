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

package io.zyient.core.mapping.model;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.utils.MetricsBase;
import io.zyient.core.mapping.readers.InputReader;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class ReaderMetrics extends MetricsBase {
    public static final String TAG_TYPE = "InputReaderType";
    public static final String METRIC_TAG_RECORDS = "RECORDS";
    public static final String METRIC_TAG_READ_BATCH = "BATCH";

    private Counter recordsCounter;

    private DistributionSummary readBatchTimer;

    public ReaderMetrics(@NonNull String engine,
                         @NonNull String name,
                         @NonNull String sourceType,
                         @NonNull BaseEnv<?> env,
                         @NonNull Class<? extends InputReader> type) {
        super(engine, name, sourceType, env);
        Map<String, String> tags = Map.of(TAG_TYPE, type.getCanonicalName());
        recordsCounter = addCounter(METRIC_TAG_RECORDS, tags);

        readBatchTimer = addTimer(METRIC_TAG_READ_BATCH, tags);
    }
}
