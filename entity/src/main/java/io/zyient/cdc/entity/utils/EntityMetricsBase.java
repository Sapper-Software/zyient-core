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

package io.zyient.cdc.entity.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.utils.MetricsBase;
import io.zyient.cdc.entity.model.DbSource;
import io.zyient.cdc.entity.schema.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class EntityMetricsBase extends MetricsBase {

    public EntityMetricsBase(@NonNull String engine,
                             @NonNull DbSource.EngineType type,
                             @NonNull String name,
                             @NonNull BaseEnv<?> env) {
        super(engine, type.name(), name, env);
    }

    public Counter addCounter(@NonNull SchemaEntity entity,
                              @NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (counters().containsKey(name)) {
            return counters().get(name);
        }
        Map<String, String> tags = EntityMetricsHelper.metricsTags(env(), engine(), sourceType(), this.name(), entity);
        return addCounter(metricsName(METRICS_COUNTER, name), tags);
    }

    public DistributionSummary addTimer(@NonNull SchemaEntity entity,
                                        @NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (timers().containsKey(name)) {
            return timers().get(name);
        }
        Map<String, String> tags = EntityMetricsHelper.metricsTags(env(), engine(), sourceType(), this.name(), entity);
        return addTimer(metricsName(METRICS_TIMER, name), tags);
    }
}
