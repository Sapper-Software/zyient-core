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

package io.zyient.base.core.stores.impl.solr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
import lombok.NonNull;
import lombok.Setter;
import org.apache.solr.client.solrj.beans.Field;

import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class SolrEntity<K extends IKey> implements IEntity<K>, VersionedEntity {
    public static final String FIELD_SOLR_ID = "id";
    public static final String FIELD_SOLR_TYPE = "class";
    public static final String FIELD_SOLR_TIME_CREATED = "time_created";
    public static final String FIELD_SOLR_TIME_UPDATED = "time_updated";
    @Field(FIELD_SOLR_ID)
    private String _id;
    @Field(FIELD_SOLR_TYPE)
    private String _type;
    @Setter(AccessLevel.NONE)
    @JsonIgnore
    private final EntityState state = new EntityState();
    @Field(FIELD_SOLR_TIME_CREATED)
    private long createdTime;
    @Field(FIELD_SOLR_TIME_UPDATED)
    private long updatedTime;

    protected SolrEntity() {
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
        Preconditions.checkArgument(source instanceof SolrEntity<K>);
        _id = ((SolrEntity<K>) source).get_id();
        _type = getClass().getCanonicalName();
        createdTime = ((SolrEntity<K>) source).createdTime;
        updatedTime = ((SolrEntity<K>) source).updatedTime;
        state.setState(((SolrEntity<K>) source).state.getState());
        return this;
    }

    @Override
    public long version() {
        return updatedTime;
    }

    protected void clone(@NonNull SolrEntity<K> entity) {
        entity._id = _id;
        entity._type = getClass().getCanonicalName();
        entity.getState().setState(state.getState());
        entity.createdTime = createdTime;
        entity.updatedTime = updatedTime;
    }
}
