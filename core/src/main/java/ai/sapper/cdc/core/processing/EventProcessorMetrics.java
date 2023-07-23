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

package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.utils.MetricsBase;
import lombok.NonNull;

public class EventProcessorMetrics extends MetricsBase {
    public static final String METRIC_EVENTS_READ = "events_read";
    public static final String METRIC_EVENTS_PROCESSED = "events_processed";
    public static final String METRIC_EVENTS_ERROR = "events_error";
    public static final String METRIC_EVENTS_TIME = "events_process_time";
    public static final String METRIC_BATCH_TIME = "events_batch_time";

    public EventProcessorMetrics(@NonNull String engine,
                                 @NonNull String name,
                                 @NonNull String sourceType,
                                 @NonNull BaseEnv<?> env) {
        super(engine, name, sourceType, env);
        addCounter(METRIC_EVENTS_READ, null);
        addCounter(METRIC_EVENTS_PROCESSED, null);
        addCounter(METRIC_EVENTS_ERROR, null);
        addTimer(METRIC_EVENTS_TIME, null);
        addTimer(METRIC_BATCH_TIME, null);
    }
}
