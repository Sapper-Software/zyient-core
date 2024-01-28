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

package io.zyient.core.filesystem.sync.s3.process;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.executor.FatalError;
import io.zyient.base.core.processing.ProcessingState;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.filesystem.sync.s3.model.S3Event;
import io.zyient.core.messaging.InvalidMessageError;
import io.zyient.core.messaging.MessageObject;
import io.zyient.core.messaging.MessageProcessingError;
import io.zyient.core.messaging.MessagingProcessorSettings;
import io.zyient.core.messaging.aws.AwsSQSOffset;
import io.zyient.core.messaging.aws.SQSMessage;
import io.zyient.core.messaging.processing.MessageProcessorState;
import io.zyient.core.messaging.processing.aws.BaseSQSMessageProcessor;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class S3EventListener extends BaseSQSMessageProcessor<ES3EventProcessorState, S3EventOffset, String> {

    private S3EventHandler handler = null;
    private S3EventListenerSettings settings;
    private BaseEnv<?> env;
    private S3EventConsumer consumer;

    public S3EventListener() {
        super(S3EventProcessingState.class, S3EventListenerSettings.class);
    }

    public S3EventListener withHandler(@NonNull S3EventHandler handler) {
        this.handler = handler;
        return this;
    }

    public S3EventListener init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                @NonNull BaseEnv<?> env) throws ConfigurationException {
        this.env = env;
        try {
            super.init(env, config);
            settings = (S3EventListenerSettings) receiverConfig.settings();
            if (settings.getHandler() != null) {
                handler = settings.getHandler()
                        .getDeclaredConstructor()
                        .newInstance();
                handler.init(receiverConfig.config(), env);
            } else if (handler == null) {
                throw new Exception("Event handler not defined...");
            }

            return this;
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    protected void initState(@NonNull ProcessingState<ES3EventProcessorState, S3EventOffset> processingState) throws Exception {
        S3EventStateManager stateManager = (S3EventStateManager) stateManager();
        if (processingState.getOffset() == null) {
            processingState.setOffset(new S3EventOffset());
        }

        processingState.setState(ES3EventProcessorState.Running);
        processingState = stateManager.update(processingState);
    }

    @Override
    protected ProcessingState<ES3EventProcessorState, S3EventOffset> finished(@NonNull ProcessingState<ES3EventProcessorState, S3EventOffset> processingState) {
        processingState.setState(ES3EventProcessorState.Stopped);
        return processingState;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void process(@NonNull MessageObject<String, String> message,
                           @NonNull MessageProcessorState<ES3EventProcessorState, S3EventOffset, AwsSQSOffset> processorState) throws Exception {
        Preconditions.checkArgument(message instanceof SQSMessage<String>);
        try {
            String body = message.value();
            if (!Strings.isNullOrEmpty(body)) {
                Map<String, Object> data = JSONUtils.read(body, Map.class);
                List<S3Event> es = S3Event.read(data);
                if (es != null) {
                    for (S3Event event : es) {
                        event.setMessageId(message.id());
                        handler.handle(event);
                    }
                }
            } else {
                DefaultLogger.warn(String.format("[id=%s] Empty message received...",
                        ((SQSMessage<String>) message).sqsMessageId()));
            }
        } catch (InvalidMessageError | MessageProcessingError me) {
            DefaultLogger.stacktrace(me);
            DefaultLogger.error(String.format("{id=%s} Error=[%s]",
                    ((SQSMessage<String>) message).sqsMessageId(), me.getLocalizedMessage()));

        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new FatalError(t);
        }
    }

    @Override
    protected void postInit(@NonNull MessagingProcessorSettings settings) throws Exception {
        if (processingState().getOffset() == null) {
            processingState().setOffset(new S3EventOffset());
        }
        state.setState(ProcessorState.EProcessorState.Initialized);
    }

    @Override
    protected void batchStart(@NonNull MessageProcessorState<ES3EventProcessorState, S3EventOffset, AwsSQSOffset> processorState) throws Exception {

    }

    @Override
    protected void batchEnd(@NonNull MessageProcessorState<ES3EventProcessorState, S3EventOffset, AwsSQSOffset> processorState) throws Exception {

    }
}
