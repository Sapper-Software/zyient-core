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

package io.zyient.core.mapping.model;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.core.mapping.rules.RuleValidationError;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@Table(name = "tb_entity_errors")
public class EntityValidationError implements IEntity<EntityErrorsId> {
    @EmbeddedId
    private EntityErrorsId id;
    @Column(name = "rule_name")
    private String rule;
    @Column(name = "field_name")
    private String field;
    @Column(name = "error_code")
    private int errorCode;
    @Column(name = "message")
    private String message;
    @Column(name = "exception")
    private String exception;

    public EntityValidationError() {

    }

    public EntityValidationError(@NonNull String entityId,
                                 @NonNull Class<? extends IEntity<?>> entityType,
                                 @NonNull RuleValidationError error) {
        id = new EntityErrorsId();
        id.setEntityId(entityId);
        id.setEntityType(entityType.getCanonicalName());
        rule = error.rule();
        field = error.field();
        errorCode = error.errorCode();
        message = error.message();
        exception = error.inner().getLocalizedMessage();
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(EntityErrorsId key) {
        return id.compareTo(key);
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
    public IEntity<EntityErrorsId> copyChanges(IEntity<EntityErrorsId> source, Context context) throws CopyException {
        return null;
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<EntityErrorsId> clone(Context context) throws CopyException {
        return null;
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public EntityErrorsId entityKey() {
        return id;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {

    }
}
