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

package io.zyient.core.mapping.mapper;

import io.zyient.base.common.model.Context;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.mapping.Mapped;
import io.zyient.core.mapping.model.mapping.MappedResponse;
import io.zyient.core.mapping.model.mapping.SourceMap;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface MappingVisitor {
    String __CONFIG_PATH = "visitor";

    MappingVisitor configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                @NonNull BaseEnv<?> env) throws ConfigurationException;

    void visit(@NonNull Mapped me,
               @NonNull MappedResponse<?> response,
               @NonNull SourceMap source,
               @NonNull Context context);
}
