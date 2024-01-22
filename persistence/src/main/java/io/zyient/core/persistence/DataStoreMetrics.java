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

package io.zyient.core.persistence;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.utils.MetricsBase;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class DataStoreMetrics extends MetricsBase {
    public static final String TAG_TYPE = "DataStoreType";

    public static final String METRIC_TAG_READ = "READ";
    public static final String METRIC_TAG_SEARCH = "SEARCH";
    public static final String METRIC_TAG_CREATE = "CREATE";
    public static final String METRIC_TAG_UPDATE = "UPDATE";
    public static final String METRIC_TAG_DELETE = "DELETE";
    public static final String METRIC_TAG_READ_ERROR = "READ_ERROR";
    public static final String METRIC_TAG_SEARCH_ERROR = "SEARCH_ERROR";
    public static final String METRIC_TAG_CREATE_ERROR = "CREATE_ERROR";
    public static final String METRIC_TAG_UPDATE_ERROR = "UPDATE_ERROR";
    public static final String METRIC_TAG_DELETE_ERROR = "DELETE_ERROR";

    private Counter readCounter;
    private Counter searchCounter;
    private Counter createCounter;
    private Counter updateCounter;
    private Counter deleteCounter;

    private DistributionSummary readTimer;
    private DistributionSummary searchTimer;
    private DistributionSummary createTimer;
    private DistributionSummary updateTimer;
    private DistributionSummary deleteTimer;

    private Counter readCounterError;
    private Counter searchCounterError;
    private Counter createCounterError;
    private Counter updateCounterError;
    private Counter deleteCounterError;

    public DataStoreMetrics(@NonNull String engine,
                            @NonNull String name,
                            @NonNull String sourceType,
                            @NonNull BaseEnv<?> env,
                            @NonNull Class<?> type) {
        super(engine, name, sourceType, env);
        Map<String, String> tags = Map.of(TAG_TYPE, type.getCanonicalName());
        readCounter = addCounter(METRIC_TAG_READ, tags);
        searchCounter = addCounter(METRIC_TAG_SEARCH, tags);
        createCounter = addCounter(METRIC_TAG_CREATE, tags);
        updateCounter = addCounter(METRIC_TAG_UPDATE, tags);
        deleteCounter = addCounter(METRIC_TAG_DELETE, tags);

        readTimer = addTimer(METRIC_TAG_READ, tags);
        searchTimer = addTimer(METRIC_TAG_SEARCH, tags);
        createTimer = addTimer(METRIC_TAG_CREATE, tags);
        updateTimer = addTimer(METRIC_TAG_UPDATE, tags);
        deleteTimer = addTimer(METRIC_TAG_DELETE, tags);

        readCounterError = addCounter(METRIC_TAG_READ_ERROR, tags);
        searchCounterError = addCounter(METRIC_TAG_SEARCH_ERROR, tags);
        createCounterError = addCounter(METRIC_TAG_CREATE_ERROR, tags);
        updateCounterError = addCounter(METRIC_TAG_UPDATE_ERROR, tags);
        deleteCounterError = addCounter(METRIC_TAG_DELETE_ERROR, tags);
    }
}
