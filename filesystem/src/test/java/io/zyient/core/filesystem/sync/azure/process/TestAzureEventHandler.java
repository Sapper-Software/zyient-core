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

package io.zyient.core.filesystem.sync.azure.process;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.filesystem.sync.azure.model.AzureFSEvent;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class TestAzureEventHandler implements AzureFSEventHandler {
    @Override
    public AzureFSEventHandler init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull BaseEnv<?> env) throws ConfigurationException {
        return this;
    }

    @Override
    public void handle(@NonNull AzureFSEvent event) throws Exception {
        String json = JSONUtils.asString(event);
        DefaultLogger.info(String.format("Received event: [%s]", json));
    }
}
