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

package io.zyient.base.core.messaging.aws;

import io.zyient.base.core.messaging.MessageObject;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class SQSMessage<M> extends MessageObject<String, M> {
    public static final String HEADER_MESSAGE_KEY = "ZYC_HEADER_KEY";
    public static final String HEADER_MESSAGE_TIMESTAMP = "ZYC_HEADER_TIMESTAMP";

    private String sqsMessageId;
    private long sequence;
    private long timestamp;
}
