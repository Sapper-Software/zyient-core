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

package io.zyient.intake.flow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.zyient.base.common.AbstractState;
import io.zyient.base.common.StateException;

public class TaskGroupState extends AbstractState<ETaskGroupState> {
    public TaskGroupState() {
       super(ETaskGroupState.Error, ETaskGroupState.Unknown);
    }

    @JsonIgnore
    public boolean isRunning() {
        return (getState() == ETaskGroupState.Running);
    }

    public void checkState(Class<?> caller, ETaskGroupState expected) throws StateException {
        if (getState() != expected) {
            throw new StateException(String.format("State Error : [expected=%s][current=%s]",
                    expected.name(), getState().name()));
        }
    }
}
