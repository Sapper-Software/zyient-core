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

package io.zyient.base.core.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.zyient.base.core.BaseEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class MetricsBase {
    public static final String TAG_ENV_NAME = "ENV_NAME";
    public static final String TAG_DB_TYPE = "DB_TYPE";
    public static final String TAG_ENGINE = "ENGINE";
    public static final String TAG_INSTANCE_NAME = "PIPELINE";
    public static final String METRICS_TIMER = "timer";
    public static final String METRICS_COUNTER = "counter";
    private final BaseEnv<?> env;
    private final Map<String, Counter> counters = new HashMap<>();
    private final Map<String, DistributionSummary> timers = new HashMap<>();
    private final String engine;
    private final String name;
    private final String sourceType;

    public MetricsBase(@NonNull String engine,
                       @NonNull String name,
                       @NonNull String sourceType,
                       @NonNull BaseEnv<?> env) {
        this.env = env;
        this.engine = engine;
        this.name = name;
        this.sourceType = sourceType;
    }

    public synchronized Counter addCounter(@NonNull String name,
                              Map<String, String> tags) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (counters.containsKey(name)) {
            return counters.get(name);
        }
        name = metricsName(METRICS_COUNTER, name);
        tags = metricsTags(env, engine, sourceType, this.name, tags);
        String[] tgs = tags(tags);
        Counter counter = Counter
                .builder(name)
                .description(String.format("[%s.%s] Timed metric for [%s]",
                        env.name(),
                        this.name,
                        name))
                .tags(tgs)
                .register(BaseEnv.registry());
        counters.put(name, counter);
        return counter;
    }

    public Counter getCounter(String name) {
        name = metricsName(METRICS_COUNTER, name);
        return counters.get(name);
    }


    public synchronized DistributionSummary addTimer(@NonNull String name,
                                        Map<String, String> tags) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (timers.containsKey(name)) {
            return timers.get(name);
        }
        name = metricsName(METRICS_TIMER, name);
        tags = metricsTags(env, engine, sourceType, this.name, tags);
        String[] tgs = tags(tags);
        DistributionSummary timer = DistributionSummary.builder(name)
                .description(String.format("[%s.%s] Timed metric for [%s]",
                        env.name(),
                        this.name,
                        name))
                .tags(tgs)
                .register(BaseEnv.registry());
        timers.put(name, timer);
        return timer;
    }

    public DistributionSummary getTimer(String name) {
        name = metricsName(METRICS_TIMER, name);
        return timers.get(name);
    }


    private String[] tags(Map<String, String> map) {
        String[] tags = new String[map.size() * 2];
        int ii = 0;
        for (String key : map.keySet()) {
            tags[ii] = key;
            ii++;
            tags[ii] = map.get(key);
            ii++;
        }
        return tags;
    }

    public String metricsName(@NonNull String type,
                              @NonNull String name) {
        return String.format("%s_%s", type, name);
    }

    public static Map<String, String> metricsTags(@NonNull BaseEnv<?> env,
                                                  @NonNull String engine,
                                                  @NonNull String type,
                                                  @NonNull String name,
                                                  Map<String, String> parts) {

        Map<String, String> map = null;
        if (parts == null) {
            map = new HashMap<>();
        } else {
            map = new HashMap<>(parts);
        }
        map.put(TAG_ENV_NAME, env.name());
        map.put(TAG_INSTANCE_NAME, name);
        map.put(TAG_DB_TYPE, type);
        map.put(TAG_ENGINE, engine);
        return map;
    }
}
