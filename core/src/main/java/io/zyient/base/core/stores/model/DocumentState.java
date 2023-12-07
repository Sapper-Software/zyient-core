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

package io.zyient.base.core.stores.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.core.content.impl.db.converters.GenericJsonConverter;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@Embeddable
public abstract class DocumentState<E extends Enum<?>> {
    private final E errorState;
    private final E newState;
    private final E availableState;
    @Enumerated(EnumType.STRING)
    @Column(name = "doc_state")
    private E state;
    @Convert(converter = GenericJsonConverter.class)
    @Column(name = "error")
    private Throwable error;

    protected DocumentState(@NonNull E errorState,
                            @NonNull E newState,
                            @NonNull E availableState) {
        this.errorState = errorState;
        this.newState = newState;
        this.availableState = availableState;
        state = newState;
    }

    public DocumentState<E> error(@NonNull Throwable error) {
        this.state = errorState;
        this.error = error;
        return this;
    }

    @SuppressWarnings("unchecked")
    public DocumentState<E> create() throws Exception {
        DocumentState<E> state = getClass().getDeclaredConstructor()
                .newInstance();
        state.setState(newState);
        return state;
    }

    public DocumentState<E> available() {
        state = availableState;
        return this;
    }
}
