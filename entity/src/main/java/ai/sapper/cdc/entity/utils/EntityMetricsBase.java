package ai.sapper.cdc.entity.utils;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.utils.MetricsBase;
import ai.sapper.cdc.entity.model.DbSource;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
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
