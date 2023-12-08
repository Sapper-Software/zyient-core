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

package io.zyient.core.messaging.kafka.builders;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.messaging.builders.MessageReceiverSettings;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <receiver> -- Or root name
 *         <type>[EConnectionType]</type>
 *         <connection>[Message Connection name]</connection>
 *         <offset>
 *             <manager>[Offset Manager name]</manager>
 *         </offset>
 *         <batchSize>[Receive batch size, default = -1(ignore)]</batchSize>
 *         <receiverTimeout>[Receiver timeout, default = -1(ignore)]</receiverTimeout>
 *         <errorQueue>
 *             <class>[Kafka Producer implementation class]</class>
 *             -- Kafka Producer settings --
 *             <type>[EConnectionType]</type>
 *             ...
 *         </errorQueue>
 *     </receiver>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaReceiverSettings extends MessageReceiverSettings {
}
