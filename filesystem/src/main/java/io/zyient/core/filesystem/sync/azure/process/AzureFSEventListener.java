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

import com.azure.storage.blob.changefeed.BlobChangefeedClient;
import com.azure.storage.blob.changefeed.BlobChangefeedClientBuilder;
import com.azure.storage.blob.changefeed.BlobChangefeedPagedResponse;
import com.azure.storage.blob.changefeed.models.BlobChangefeedEvent;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.azure.AzureFsClient;
import io.zyient.base.core.processing.ProcessingState;
import io.zyient.base.core.processing.Processor;
import io.zyient.base.core.processing.ProcessorSettings;
import io.zyient.core.filesystem.sync.EEventProcessorState;
import io.zyient.core.filesystem.sync.azure.model.AzureFSEvent;
import io.zyient.core.filesystem.sync.azure.model.AzureFSEventOffset;
import io.zyient.core.filesystem.sync.azure.model.AzureFSProcessingState;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class AzureFSEventListener extends Processor<EEventProcessorState, AzureFSEventOffset> {
    private AzureFSEventListenerSettings settings;
    private AzureFsClient client;
    private BlobChangefeedClient listener;
    private AzureFSEventHandler handler;
    private Pattern regex = null;

    protected AzureFSEventListener() {
        super(AzureFSProcessingState.class);
    }

    @Override
    public Processor<EEventProcessorState, AzureFSEventOffset> init(@NonNull BaseEnv<?> env,
                                                                    @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                                    String path) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig,
                    ProcessorSettings.__CONFIG_PATH,
                    AzureFSEventListenerSettings.class);
            reader.read();
            settings = (AzureFSEventListenerSettings) reader.settings();
            super.init(settings, env);
            client = env.connectionManager()
                    .getConnection(settings.getConnection(), AzureFsClient.class);
            if (client == null) {
                throw new Exception(String.format("Azure FS Connection not found. [name=%s]",
                        settings.getConnection()));
            }
            listener = new BlobChangefeedClientBuilder(client.client())
                    .buildClient();
            if (settings.getHandler() != null) {
                handler = settings.getHandler()
                        .getDeclaredConstructor()
                        .newInstance();
                handler.init(reader.config(), env);
            } else if (handler == null) {
                throw new Exception("Event handler not defined...");
            }
            if (!Strings.isNullOrEmpty(settings.getPathFilter())) {
                regex = Pattern.compile(settings.getPathFilter());
            }
            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            state.error(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    protected void initState(@NonNull ProcessingState<EEventProcessorState, AzureFSEventOffset> processingState) throws Exception {
        Preconditions.checkArgument(processingState instanceof AzureFSProcessingState);
        AzureFSEventStateManager stateManager = (AzureFSEventStateManager) stateManager();
        if (processingState.getOffset() == null) {
            processingState.setOffset(new AzureFSEventOffset());
        }
        processingState.setState(EEventProcessorState.Running);
        stateManager.update(name(), processingState);
    }

    @Override
    protected ProcessingState<EEventProcessorState, AzureFSEventOffset> finished(@NonNull ProcessingState<EEventProcessorState, AzureFSEventOffset> processingState) {
        Preconditions.checkArgument(processingState instanceof AzureFSProcessingState);
        processingState.setState(EEventProcessorState.Stopped);
        return processingState;
    }

    @Override
    protected void doRun(boolean runOnce) throws Throwable {
        Preconditions.checkArgument(!runOnce);
        while (state.isRunning()) {
            boolean sleep = true;
            __lock().lock();
            try {
                if (!state.isPaused()) {
                    AzureFSProcessingState state = (AzureFSProcessingState) processingState();
                    Iterable<BlobChangefeedPagedResponse> pages = null;
                    if (Strings.isNullOrEmpty(state.getOffset().getToken())) {
                        pages = listener.getEvents().iterableByPage();
                    } else {
                        pages = listener.getEvents(state.getOffset().getToken()).iterableByPage();
                    }
                    int count = 0;
                    if (pages != null) {
                        for (BlobChangefeedPagedResponse page : pages) {
                            List<BlobChangefeedEvent> events = page.getValue();
                            if (events != null && !events.isEmpty()) {
                                for (BlobChangefeedEvent event : events) {
                                    AzureFSEvent e = AzureFSEvent.parse(event);
                                    if (regex != null) {
                                        if (!Strings.isNullOrEmpty(e.getPath())) {
                                            Matcher m = regex.matcher(e.getPath());
                                            if (!m.matches()) {
                                                DefaultLogger.debug(String.format("Ignored file: [container=%s][path=%s]",
                                                        e.getContainer(), e.getPath()));
                                                continue;
                                            }
                                        }
                                    }
                                    DefaultLogger.debug(String.format("Handling file: [container=%s][path=%s]",
                                            e.getContainer(), e.getPath()));
                                    handler.handle(e);
                                }
                            }
                            state.getOffset().setToken(page.getContinuationToken());
                        }
                        updateState();
                        sleep = false;
                    }
                }
            } finally {
                __lock().unlock();
            }
            if (sleep) {
                RunUtils.sleep(settings.getPollingInterval().normalized());
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!processingState().hasError())
            processingState().setState(EEventProcessorState.Stopped);
        if (listener != null) {
            listener = null;
        }
    }
}
