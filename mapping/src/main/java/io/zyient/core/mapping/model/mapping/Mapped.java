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

package io.zyient.core.mapping.model.mapping;

import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.ConfigPath;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.mapper.MappingVisitor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@ConfigPath(path = "map")
public abstract class Mapped {
    @Config(name = "sequence", type = Integer.class)
    private int sequence;
    @Config(name = "visitor", required = false, type = Class.class)
    private Class<? extends MappingVisitor> visitorType;
    private MappingVisitor visitor;

    public static Mapped read(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                              @NonNull Class<? extends Mapped> type,
                              @NonNull BaseEnv<?> env) throws Exception {
        Mapped mapped = ConfigReader.read(xmlConfig, type);
        if (mapped.visitorType != null) {
            HierarchicalConfiguration<ImmutableNode> node = xmlConfig.configurationAt(MappingVisitor.__CONFIG_PATH);
            mapped.visitor = mapped.visitorType.getDeclaredConstructor()
                    .newInstance()
                    .configure(node, env);
        }
        return mapped;
    }
}
