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

package io.zyient.core.caseflow;

import io.zyient.base.common.model.Context;
import io.zyient.core.caseflow.errors.CaseAuthorizationError;
import io.zyient.core.caseflow.model.Case;
import io.zyient.core.caseflow.model.CaseAction;
import io.zyient.core.caseflow.model.CaseState;
import io.zyient.core.caseflow.model.UserOrRole;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface ActionAuthorization<S extends CaseState<?>> {
    void configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    void authorize(Case<S, ?, ?> caseObject,
                   @NonNull CaseAction action,
                   @NonNull UserOrRole actor,
                   Context context) throws CaseAuthorizationError;

    void checkAssignment(@NonNull Case<S, ?, ?> caseObject,
                         @NonNull UserOrRole assignTo,
                         Context context) throws CaseAuthorizationError;
}
