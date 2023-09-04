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

package io.zyient.base.common.model.services;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class BasicResponse<T> {
    private EResponseState state;
    private Throwable error;
    private T response;

    public BasicResponse() {}

    public BasicResponse(@NonNull EResponseState state, T response) {
        this.state = state;
        this.response = response;
    }

    public BasicResponse<T> withError(@NonNull Throwable error) {
        this.error = error;
        return this;
    }
}
