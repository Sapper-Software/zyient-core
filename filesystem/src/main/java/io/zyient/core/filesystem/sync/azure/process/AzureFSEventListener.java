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
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.processing.ProcessingState;
import io.zyient.base.core.processing.Processor;
import io.zyient.core.filesystem.sync.EEventProcessorState;
import io.zyient.core.filesystem.sync.azure.model.AzureFSEventOffset;
import io.zyient.core.filesystem.sync.azure.model.AzureFSProcessingState;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class AzureFSEventListener extends Processor<EEventProcessorState, AzureFSEventOffset> {
    private AzureFSEventListenerSettings settings;

    protected AzureFSEventListener() {
        super(AzureFSProcessingState.class);
    }

    @Override
    public Processor<EEventProcessorState, AzureFSEventOffset> init(@NonNull BaseEnv<?> env,
                                                                    @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                                    String path) throws ConfigurationException {
        try {
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    protected void initState(@NonNull ProcessingState<EEventProcessorState, AzureFSEventOffset> processingState) throws Exception {

    }

    @Override
    protected ProcessingState<EEventProcessorState, AzureFSEventOffset> finished(@NonNull ProcessingState<EEventProcessorState, AzureFSEventOffset> processingState) {
        return processingState;
    }

    @Override
    protected void doRun(boolean runOnce) throws Throwable {

    }

    @Override
    public void close() throws IOException {

    }
}
