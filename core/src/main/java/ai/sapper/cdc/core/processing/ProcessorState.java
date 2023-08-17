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

package ai.sapper.cdc.core.processing;

import ai.sapper.cdc.common.AbstractEnvState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ProcessorState extends AbstractEnvState<ProcessorState.EProcessorState> {
    public ProcessorState() {
        super(EProcessorState.Error, EProcessorState.Initialized);
        setState(EProcessorState.Unknown);
    }

    @JsonIgnore
    public boolean isInitialized() {
        return (getState() == EProcessorState.Initialized
                || getState() == EProcessorState.Running);
    }

    @JsonIgnore
    @Override
    public boolean isAvailable() {
        return (getState() == EProcessorState.Running || getState() == EProcessorState.Paused);
    }

    @JsonIgnore
    public boolean isPaused() {
        return (getState() == EProcessorState.Paused);
    }

    @JsonIgnore
    public boolean isRunning() {
        return (getState() == EProcessorState.Running);
    }

    @JsonIgnore
    @Override
    public boolean isTerminated() {
        return (getState() == EProcessorState.Stopped || hasError());
    }

    public void check(@Nonnull EProcessorState state) throws Exception {
        if (state != getState()) {
            throw new Exception(
                    String.format("Invalid Processor state: [expected=%s][state=%s]",
                            state.name(), getState().name()));
        }
    }

    public enum EProcessorState {
        Unknown, Initialized, Running, Stopped, Error, Paused;
    }
}
