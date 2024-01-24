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

package io.zyient.base.core.decisions.builder;

import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.decisions.EvaluationTree;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface EvaluationTreeBuilder<T, R> {
    String __CONFIG_PATH = "builder";

    EvaluationTreeBuilder<T, R> configure(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                          @NonNull BaseEnv<?> env) throws ConfigurationException;

    EvaluationTree<T, R> build() throws Exception;
}
