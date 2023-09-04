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

package io.zyient.base.common;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public abstract class AbstractState<T extends Enum<?>> {
    private T state;
    @Setter(AccessLevel.NONE)
    private Throwable error;
    private final T errorState;
    private final T initState;

    public AbstractState(@NonNull T errorState,
                         @NonNull T initState) {
        this.errorState = errorState;
        this.initState = initState;
        this.state = initState;
    }

    public AbstractState<T> error(@NonNull Throwable error) {
        state = errorState;
        this.error = error;
        return this;
    }

    public boolean hasError() {
        return (state == errorState);
    }

    public void clear() {
        state = initState;
        error = null;
    }
}
