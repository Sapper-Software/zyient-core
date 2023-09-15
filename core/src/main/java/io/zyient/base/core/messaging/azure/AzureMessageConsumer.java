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

package io.zyient.base.core.messaging.azure;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.units.TimeUnitValue;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.azure.ServiceBusConsumerConnection;
import io.zyient.base.core.messaging.MessageObject;
import io.zyient.base.core.messaging.MessageReceiver;
import io.zyient.base.core.messaging.MessagingError;
import io.zyient.base.core.messaging.aws.BaseSQSConsumer;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.state.OffsetState;
import lombok.NonNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public abstract class AzureMessageConsumer<M> extends MessageReceiver<String, M> {
    private Queue<MessageObject<String, M>> cache = null;
    private final Map<String, AzureMessageOffsetData> offsetMap = new HashMap<>();
    private ServiceBusConsumerConnection consumer;
    private AzureMessagingStateManager stateManager;
    private AzureMessagingConsumerState state;
    private TimeUnitValue ackTimeout = new TimeUnitValue(2 * 60 * 1000, TimeUnit.MILLISECONDS);
    private int batchSize;

    public AzureMessageConsumer<M> withBatchSize(int batchSize) {
        Preconditions.checkArgument(batchSize > 0);
        this.batchSize = batchSize;
        return this;
    }

    public AzureMessageConsumer<M> withAckTimeout(@NonNull TimeUnitValue value) {
        ackTimeout = value;
        return this;
    }

    @Override
    public void ack(@NonNull List<String> messageIds) throws MessagingError {
        Preconditions.checkState(state().isAvailable());
        boolean found = false;
        for (String id : messageIds) {
            if (offsetMap.containsKey(id)) {
                offsetMap.get(id).acked(true);
                found = true;
            } else {
                DefaultLogger.warn(String.format("[%s] Message not found. [id=%s]", queueUrl, id));
            }
        }
        if (found) {
            commit();
        }
    }

    @Override
    public OffsetState<?, ?> currentOffset(Context context) throws MessagingError {
        if (!stateful())
            return null;
        return state;
    }

    @Override
    public void close() throws IOException {
        if (state().isAvailable()) {
            state().setState(ProcessorState.EProcessorState.Stopped);
        }
        if (cache != null) {
            cache.clear();
            cache = null;
        }
    }

    protected abstract M deserialize(byte[] message) throws MessagingError;
}
