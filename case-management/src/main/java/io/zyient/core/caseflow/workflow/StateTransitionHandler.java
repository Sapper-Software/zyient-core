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

package io.zyient.core.caseflow.workflow;

import io.zyient.core.caseflow.CaseManager;
import io.zyient.core.caseflow.errors.CaseActionException;
import io.zyient.core.caseflow.model.Case;
import io.zyient.core.caseflow.model.CaseDocument;
import io.zyient.core.caseflow.model.CaseState;
import io.zyient.core.persistence.env.DataStoreEnv;
import io.zyient.core.persistence.model.DocumentState;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;

public interface StateTransitionHandler<P extends Enum<P>, S extends CaseState<P>, E extends DocumentState<?>, T extends CaseDocument<E, T>> extends Closeable {
    String __CONFIG_PATH = "handler";

    String name();

    StateTransitionHandler<P, S, E, T> withCaseManager(@NonNull CaseManager<P, S, E, T> caseManager);

    StateTransitionHandler<P, S, E, T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                 @NonNull DataStoreEnv<?> env) throws ConfigurationException;

    void handleStateTransition(@NonNull P previousState,
                               @NonNull Case<P, S, E, T> caseObject) throws CaseActionException;
}
