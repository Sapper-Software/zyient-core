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

package io.zyient.core.persistence.impl.mongo;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Transient;
import dev.morphia.annotations.Version;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.ChecksumUtils;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.core.persistence.model.EntityState;
import io.zyient.core.persistence.model.VersionedEntity;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class MongoEntity<K extends IKey> implements IEntity<K>, VersionedEntity {
    @Transient
    @Setter(AccessLevel.NONE)
    private final EntityState state = new EntityState();
    @Id
    private String entityId;
    private String entityClass;
    private long createdTime;
    private long updatedTime;
    private String objectHash = null;
    @Version
    private long _version = 0;

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
            K key = entityKey();
            if (key == null) {
                errors = ValidationExceptions
                        .add(new ValidationException("Entity key is NULL"),
                                errors);
            } else {
                entityId = key.stringKey();
                if (Strings.isNullOrEmpty(entityId)) {
                    errors = ValidationExceptions
                            .add(new ValidationException("Entity key is empty string."),
                                    errors);

                }
            }
            entityClass = getClass().getCanonicalName();
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
        entityClass = getClass().getCanonicalName();
        createdTime = ((MongoEntity<K>) source).createdTime;
        updatedTime = ((MongoEntity<K>) source).updatedTime;
        state.setState(((MongoEntity<K>) source).state.getState());
        return this;
    }

    @Override
    public long version() {
        return _version;
    }

    public void postLoad() throws Exception {
        String json = JSONUtils.asString(this);
        objectHash = ChecksumUtils.generateHash(json);
        state.setState(EEntityState.Synced);
    }

    public void preSave() throws Exception {
        validate();
        String json = JSONUtils.asString(this);
        String hash = ChecksumUtils.generateHash(json);
        if (state.getState() == EEntityState.New || Strings.isNullOrEmpty(objectHash)) {
            objectHash = hash;
            setCreatedTime(System.nanoTime());
            setUpdatedTime(System.nanoTime());
        } else {
            if (hash.compareTo(objectHash) != 0) {
                state.setState(EEntityState.Updated);
                objectHash = hash;
                setUpdatedTime(System.nanoTime());
            }
        }
    }
}
