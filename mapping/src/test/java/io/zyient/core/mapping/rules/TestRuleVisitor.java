/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.rules;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.mapping.model.Customer;
import io.zyient.core.mapping.model.EvaluationStatus;
import io.zyient.core.mapping.model.mapping.MappedResponse;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class TestRuleVisitor implements RuleVisitor<MappedResponse<Customer>> {
    private String name;

    @Override
    public RuleVisitor<MappedResponse<Customer>> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                         @NonNull BaseEnv<?> env) throws ConfigurationException {
        name = xmlConfig.getString("name");
        return this;
    }

    @Override
    public void onSuccess(@NonNull MappedResponse<Customer> entity, @NonNull EvaluationStatus status) {
        String st = "Unknown";
        if (status.getStatus() != null) {
            st = status.getStatus().name();
        }
        DefaultLogger.info(String.format("[%s] Response type: status=%s [%s]",
                name, st, status.getResponse().getClass().getCanonicalName()));
    }

    @Override
    public void onError(@NonNull Throwable error, @NonNull MappedResponse<Customer> entity) {

    }
}
