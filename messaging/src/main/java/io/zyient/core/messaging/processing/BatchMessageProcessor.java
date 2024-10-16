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

package io.zyient.core.messaging.processing;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.executor.ETaskState;
import io.zyient.base.core.executor.TaskResponse;
import io.zyient.base.core.processing.ProcessingState;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.Offset;
import io.zyient.core.messaging.MessageObject;
import io.zyient.core.messaging.MessagingProcessorSettings;
import io.zyient.core.messaging.ReceiverOffset;
import jakarta.el.MethodNotFoundException;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class BatchMessageProcessor<T, K, M, E extends Enum<?>, O extends Offset, MO extends ReceiverOffset<?>>
        extends MessageProcessor<K, M, E, O, MO> {
    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    protected BatchMessageProcessor(@NonNull Class<? extends ProcessingState<E, O>> stateType,
                                    @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(stateType, settingsType);
    }

    protected BatchMessageProcessor(@NonNull Class<? extends ProcessingState<E, O>> stateType) {
        super(stateType);
    }

    @Override
    protected void handleBatch(@NonNull List<MessageObject<K, M>> batch,
                               @NonNull MessageProcessorState<E, O, MO> processorState) throws Exception {
        final List<TaskResponse<T>> responses = new ArrayList<>(batch.size());
        for (int ii = 0; ii < batch.size(); ii++) {
            MessageObject<K, M> message = batch.get(ii);
            TaskResponse<T> response = initResponse(processorState, message);
            responses.set(ii, response);
        }
        Runner runner = new Runner(this, batch, processorState, responses);
        executor.submit(runner);
        while (true) {
            synchronized (responses) {
                try {
                    responses.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    DefaultLogger.warn("Thread Interrupted");
                }
            }
            if (checkResponses(responses, processorState)) {
                break;
            }
        }
    }

    private boolean checkResponses(final List<TaskResponse<T>> responses,
                                   final @NonNull MessageProcessorState<E, O, MO> processorState) throws Exception {
        int count = 0;
        for (TaskResponse<T> response : responses) {
            if (response.state() == ETaskState.ERROR) {
                checkError(response);
                count++;
            } else if (response.state() == ETaskState.DONE) {
                count++;
            }
        }
        return (count == responses.size());
    }

    @Override
    protected void process(@NonNull MessageObject<K, M> message,
                           @NonNull MessageProcessorState<E, O, MO> processorState) throws Exception {
        throw new MethodNotFoundException("Should not be called...");
    }

    protected abstract void checkError(TaskResponse<T> response) throws Exception;

    protected abstract void process(final @NonNull List<MessageObject<K, M>> batch,
                                    final @NonNull MessageProcessorState<E, O, MO> processorState,
                                    final @NonNull List<TaskResponse<T>> responses) throws Exception;

    protected abstract MessageTaskResponse<T, K, M> initResponse(final @NonNull MessageProcessorState<E, O, MO> processorState,
                                                                 final @NonNull MessageObject<K, M> message);

    @Override
    public ProcessorState.EProcessorState stop() {
        ProcessorState.EProcessorState state = super.stop();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        return state;
    }

    private class Runner implements Runnable {
        private final BatchMessageProcessor<T, K, M, E, O, MO> processor;
        private final List<MessageObject<K, M>> batch;
        private final MessageProcessorState<E, O, MO> processorState;
        private final List<TaskResponse<T>> responses;

        public Runner(final BatchMessageProcessor<T, K, M, E, O, MO> processor,
                      final List<MessageObject<K, M>> batch,
                      MessageProcessorState<E, O, MO> processorState,
                      List<TaskResponse<T>> responses) {
            this.processor = processor;
            this.batch = batch;
            this.processorState = processorState;
            this.responses = responses;
        }

        @Override
        public void run() {
            try {
                processor.process(batch, processorState, responses);
            } catch (Throwable t) {
                DefaultLogger.error(t.getLocalizedMessage());
                DefaultLogger.stacktrace(t);
                processorState.error(t);
            }
        }
    }
}
