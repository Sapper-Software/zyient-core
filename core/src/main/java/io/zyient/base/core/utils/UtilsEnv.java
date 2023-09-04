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

package io.zyient.base.core.utils;

import io.zyient.base.common.AbstractEnvState;
import io.zyient.base.core.BaseEnv;
import lombok.NonNull;

public class UtilsEnv extends BaseEnv<UtilsEnv.EUtilsState> {
    public enum EUtilsState {
        Unknown, Available, Disposed, Error
    }

    public static class UtilsState extends AbstractEnvState<EUtilsState> {

        public UtilsState() {
            super(EUtilsState.Error, EUtilsState.Unknown);
        }

        @Override
        public boolean isAvailable() {
            return getState() == EUtilsState.Available;
        }

        @Override
        public boolean isTerminated() {
            return (getState() == EUtilsState.Disposed || hasError());
        }
    }

    public UtilsEnv(@NonNull String name) {
        super(name);
    }
}
