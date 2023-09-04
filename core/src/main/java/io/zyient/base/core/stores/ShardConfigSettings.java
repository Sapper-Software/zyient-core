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

package io.zyient.base.core.stores;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.stores.annotations.IShardProvider;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@ConfigPath(path = "shard")
public class ShardConfigSettings extends Settings {
    @Config(name = "provider", type = Class.class)
    private Class<? extends IShardProvider> provider;
    @Config(name = "entityType", type = Class.class)
    private Class<? extends IShardedEntity<?, ?>> entityType;
    @Config(name = "shards")
    private String shardMap;
    private Map<Integer, String> shards = new HashMap<>();
    @JsonIgnore
    private EConfigSource source;

    public ShardConfigSettings parse() throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(shardMap));
        String[] parts = shardMap.split(";");
        for (String part : parts) {
            String[] pp = part.split("=");
            if (pp.length != 2) {
                throw new Exception(String.format("Invalid shard config. [value=%s]", part));
            }
            shards.put(Integer.parseInt(pp[1]), pp[0]);
        }
        return this;
    }
}
