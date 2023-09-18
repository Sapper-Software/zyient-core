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

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class AzureMessageOffsetData {
    private String key;
    private ServiceBusReceivedMessage message;
    private AzureMessageOffsetValue index;
    private boolean acked = false;

    public AzureMessageOffsetData(@NonNull String key,
                                  @NonNull AzureMessageOffsetValue index,
                                  @NonNull ServiceBusReceivedMessage message) {
        Preconditions.checkArgument(index.getIndex() >= 0);
        this.index = index;
        this.key = key;
        this.message = message;
    }
}
