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

package io.zyient.base.core.stores.impl.mongo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Version;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.core.model.EntityState;
import io.zyient.base.core.stores.VersionedEntity;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class MongoEntity<K extends IKey> implements IEntity<K>, VersionedEntity {
    @Id
    private String _id;
    private String _type;
    @JsonIgnore
    @Setter(AccessLevel.NONE)
    private final EntityState state = new EntityState();
    private long createdTime;
    @Version
    private long updatedTime;

    public MongoEntity() {
        createdTime = 0;
        updatedTime = 0;
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
            _id = entityKey().stringKey();
            if (Strings.isNullOrEmpty(_id)) {
                errors = ValidationExceptions
                        .add(new ValidationException("Key String is NULL/empty [field=_id]"), errors);
            }
            _type = getClass().getCanonicalName();
            if (createdTime <= 0) {
                errors = ValidationExceptions
                        .add(new ValidationException("Created time is not set."), errors);
            }
            if (updatedTime <= 0 || updatedTime < createdTime) {
                errors = ValidationExceptions
                        .add(new ValidationException("Updated time is not set."), errors);
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
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    public abstract ValidationExceptions doValidate(ValidationExceptions errors) throws ValidationExceptions;

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
        Preconditions.checkArgument(source instanceof MongoEntity<K>);
        _id = ((MongoEntity<K>) source)._id;
        _type = getClass().getCanonicalName();
        createdTime = ((MongoEntity<K>) source).createdTime;
        updatedTime = ((MongoEntity<K>) source).updatedTime;
        state.setState(((MongoEntity<K>) source).state.getState());
        return this;
    }

    @Override
    public long version() {
        return updatedTime;
    }
}
