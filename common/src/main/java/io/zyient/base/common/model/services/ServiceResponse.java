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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ServiceResponse<T> {
    private ServiceResponseState state = new ServiceResponseState();
    private T entity;

    public ServiceResponse() {
    }

    public ServiceResponse(@NonNull Throwable error) {
        withError(error);
    }

    public ServiceResponse(@NonNull EResponseState state,
                           T entity) throws Exception {
        this.state.setState(state);
        if (state == EResponseState.Success) {
            if (entity == null) {
                throw new Exception("Entity not specified for success call.");
            }
        }
        this.entity = entity;
    }

    public ServiceResponse<T> withError(@NonNull Throwable error) {
        state.error(error);
        return this;
    }
}
