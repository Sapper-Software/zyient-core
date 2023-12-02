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

package io.zyient.base.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.stores.VersionedEntity;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@MappedSuperclass
public abstract class BaseEntity<K extends IKey> implements IEntity<K>, VersionedEntity {
    @Setter(AccessLevel.NONE)
    @Transient
    @JsonIgnore
    private final EntityState state = new EntityState();
    @Column(name = "time_created")
    private long createdTime;
    @Column(name = "time_updated")
    @Version
    private long updatedTime;

    public BaseEntity() {
        state.setState(EEntityState.Syncing);
    }


    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        try {
            ValidationExceptions errors = null;
            if (state.inState(new EEntityState[]{EEntityState.Unknown, EEntityState.Error})) {
                errors = ValidationExceptions
                        .add(new ValidationException(String.format("Invalid entity state. [state=%s]",
                                        state.getState().name())),
                                errors);
            }
            errors = doValidate(errors);
            if (errors != null) {
                throw errors;
            }
        } catch (ValidationExceptions ex) {
            state.error(ex);
            throw ex;
        }
    }

    /**
     * Copy the changes from the specified source entity
     * to this instance.
     * <p>
     * All properties other than the Key will be copied.
     * Copy Type:
     * Primitive - Copy
     * String - Copy
     * Enum - Copy
     * Nested Entity - Copy Recursive
     * Other Objects - Copy Reference.
     *
     * @param source  - Source instance to Copy from.
     * @param context - Execution context.
     * @return - Copied Entity instance.
     * @throws CopyException
     */
    @Override
    public IEntity<K> copyChanges(IEntity<K> source, Context context) throws CopyException {
        Preconditions.checkArgument(source instanceof BaseEntity<?>);
        BaseEntity<K> entity = (BaseEntity<K>) source;
        this.state.setState(entity.state.getState());
        this.updatedTime = entity.updatedTime;
        this.createdTime = entity.createdTime;
        return this;
    }


    public void clone(@NonNull BaseEntity<K> entity, @NonNull EEntityState state) throws CopyException {
        this.state.setState(state);
        this.createdTime = System.nanoTime();
        this.updatedTime = System.nanoTime();
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    public abstract ValidationExceptions doValidate(ValidationExceptions errors) throws ValidationExceptions;


    @Override
    public long version() {
        return updatedTime;
    }
}