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

package ai.sapper.cdc.entity.utils;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.utils.MetricsBase;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import lombok.NonNull;

import java.util.Map;

public class EntityMetricsHelper {
    public static final String TAG_DOMAIN = "DOMAIN";
    public static final String TAG_ENTITY = "ENTITY";

    public static Map<String, String> metricsTags(@NonNull BaseEnv<?> env,
                                                  @NonNull String engine,
                                                  @NonNull String type,
                                                  @NonNull String name,
                                                  @NonNull SchemaEntity entity) {
        return Map.of(MetricsBase.TAG_ENV_NAME, env.name(),
                MetricsBase.TAG_INSTANCE_NAME, name,
                MetricsBase.TAG_DB_TYPE, type,
                MetricsBase.TAG_ENGINE, engine,
                TAG_DOMAIN, entity.getDomain(),
                TAG_ENTITY, entity.getEntity());
    }
}
