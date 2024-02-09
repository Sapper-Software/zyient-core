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
import io.zyient.core.mapping.pipeline.Pipeline;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class SourcePipelineMetrics extends MetricsBase {
    public static final String TAG_TYPE = "PipelineType";
    public static final String METRIC_TAG_RECORDS = "RECORDS";
    public static final String METRIC_TAG_PROCESSED = "PROCESSED";
    public static final String METRIC_TAG_ERRORS = "ERRORS";

    private Counter recordsCounter;
    private Counter processedCounter;
    private Counter errorsCounter;

    private DistributionSummary processTimer;

    public SourcePipelineMetrics(@NonNull String engine,
                                 @NonNull String name,
                                 @NonNull String sourceType,
                                 @NonNull BaseEnv<?> env,
                                 @NonNull Class<? extends Pipeline> type) {
        super(engine, name, sourceType, env);
        Map<String, String> tags = Map.of(TAG_TYPE, type.getCanonicalName());
        recordsCounter = addCounter(METRIC_TAG_RECORDS, tags);
        processedCounter = addCounter(METRIC_TAG_PROCESSED, tags);
        errorsCounter = addCounter(METRIC_TAG_ERRORS, tags);

        processTimer = addTimer(METRIC_TAG_RECORDS, tags);
    }
}
