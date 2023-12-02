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

package io.zyient.base.core.content.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Embeddable
@Getter
@Setter
public abstract class DocumentState<E extends Enum<?>> {
    @Transient
    private final E errorState;
    @Transient
    private final E newState;

    @Column(name = "doc_state")
    @Enumerated(EnumType.STRING)
    private E state;
    @Transient
    private Throwable error;

    protected DocumentState(@NonNull E errorState, @NonNull E newState) {
        this.errorState = errorState;
        this.newState = newState;
    }

    public DocumentState<E> error(@NonNull Throwable error) {
        this.state = errorState;
        this.error = error;
        return this;
    }

    public DocumentState<E> create() throws Exception {
        DocumentState<E> state = getClass().getDeclaredConstructor()
                .newInstance();
        state.setState(newState);
        return state;
    }
}
