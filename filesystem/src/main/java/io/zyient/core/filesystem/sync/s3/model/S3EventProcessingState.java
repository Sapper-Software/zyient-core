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

package io.zyient.core.filesystem.sync.s3.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.filesystem.sync.EEventProcessorState;
import io.zyient.core.messaging.aws.AwsSQSOffset;
import io.zyient.core.messaging.processing.MessageProcessorState;
import lombok.NonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class S3EventProcessingState extends MessageProcessorState<EEventProcessorState, S3EventOffset, AwsSQSOffset> {
    public S3EventProcessingState() {
        super(EEventProcessorState.Error, EEventProcessorState.Unknown, "s3-events");
    }

    public S3EventProcessingState(@NonNull MessageProcessorState<EEventProcessorState, S3EventOffset, AwsSQSOffset> state) {
        super(state);
    }
}
