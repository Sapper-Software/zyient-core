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

package io.zyient.core.caseflow.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.persistence.impl.rdbms.converters.GenericJsonConverter;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@MappedSuperclass
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class CaseState<E extends Enum<?>> {
    @Transient
    private final E errorState;
    @Transient
    private final E newState;
    @Transient
    private final E deletedState;
    @Transient
    private final E closedState;

    @Enumerated(EnumType.STRING)
    @Column(name = "case_state")
    private E state;
    @Column(name = "error")
    @Convert(converter = GenericJsonConverter.class)
    private Throwable error;

    protected CaseState(@NonNull E errorState,
                        @NonNull E newState,
                        @NonNull E deletedState,
                        @NonNull E closedState) {
        this.errorState = errorState;
        this.newState = newState;
        this.deletedState = deletedState;
        this.closedState = closedState;
    }

    public CaseState<E> error(@NonNull Throwable error) {
        this.state = errorState;
        this.error = error;
        return this;
    }

    @SuppressWarnings("unchecked")
    public CaseState<E> createInstance() throws Exception {
        CaseState<E> state = getClass().getDeclaredConstructor()
                .newInstance();
        state.setState(newState);
        return state;
    }

    public CaseState<E> delete() {
        state = deletedState;
        return this;
    }

    public boolean caseDeleted() {
        return state == deletedState;
    }

    public boolean caseClosed() {
        return state == closedState;
    }

    public boolean hasError() {
        return state == errorState;
    }

    public abstract CaseState<E> clearError();
}
