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

package io.zyient.core.mapping.env;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.processing.ProcessingState;
import io.zyient.core.messaging.chronicle.ChronicleOffset;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class DemoProcessingState extends ProcessingState<EDemoProcessingState, ChronicleOffset> {
    public DemoProcessingState() {
        super(EDemoProcessingState.Error, EDemoProcessingState.Unknown, DemoProcessingState.class.getSimpleName());
    }

    public DemoProcessingState(@NonNull ProcessingState<EDemoProcessingState, ChronicleOffset> state) {
        super(state);
    }
}
