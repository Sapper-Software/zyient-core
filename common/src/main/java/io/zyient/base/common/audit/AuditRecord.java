/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.base.common.audit;

import com.google.common.base.Strings;
import io.zyient.base.common.model.*;
import io.zyient.base.common.model.entity.IEntity;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.*;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "sys_audit_records")
public class AuditRecord implements IEntity<AuditRecordId> {
    @EmbeddedId
    private AuditRecordId id;
    @Enumerated(EnumType.STRING)
    @Column(name = "audit_type")
    private EAuditType auditType;
    @Column(name = "entity_id")
    private String entityId;
    @Column(name = "entity_data")
    private String entityData;
    @Column(name = "change_delta")
    private String changeDelta;
    @Column(name = "change_context")
    private String changeContext;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "modifiedBy", column = @Column(name = "user_id")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "timestamp"))
    })
    private ModifiedBy createdBy;

    public AuditRecord() {
    }

    public AuditRecord(@Nonnull Class<?> dataStoreType,
                       @Nonnull String dataStoreName,
                       @Nonnull Class<?> entityType) {
        id = new AuditRecordId();
        id.setDataStoreType(dataStoreType.getCanonicalName());
        id.setDataStoreName(dataStoreName);
        id.setRecordType(entityType.getCanonicalName());
        id.setRecordId(UUID.randomUUID().toString());
    }

    public AuditRecord(@Nonnull Class<?> dataStoreType,
                       @Nonnull String dataStoreName,
                       @Nonnull Class<?> entityType,
                       @Nonnull String userId) {
        id = new AuditRecordId();
        id.setDataStoreType(dataStoreType.getCanonicalName());
        id.setDataStoreName(dataStoreName);
        id.setRecordType(entityType.getCanonicalName());
        id.setRecordId(UUID.randomUUID().toString());

        createdBy = new ModifiedBy();
        createdBy.setModifiedBy(userId);
        createdBy.setTimestamp(System.currentTimeMillis());
    }

    /**
     * Get the unique Key for this entity.
     *
     * @return - Entity Key.
     */
    @Override
    public AuditRecordId entityKey() {
        return id;
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(AuditRecordId key) {
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
    public IEntity<AuditRecordId> copyChanges(IEntity<AuditRecordId> source, Context context) throws CopyException {
        return this;
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<AuditRecordId> clone(Context context) throws CopyException {
        return this;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        if (id == null) {
            errors = ValidationExceptions.add(new ValidationException("Record ID is null."), errors);
        } else if (Strings.isNullOrEmpty(id.getRecordType())) {
            errors = ValidationExceptions.add(new ValidationException("Invalid Record ID: Entity record type is NULL/Empty."), errors);
        }
        if (createdBy == null) {
            errors = ValidationExceptions.add(new ValidationException("Created By is null."), errors);
        } else if (Strings.isNullOrEmpty(createdBy.getModifiedBy())) {
            errors = ValidationExceptions.add(new ValidationException("Invalid Created By: User ID is null/empty."), errors);
        }
        if (auditType == null) {
            errors = ValidationExceptions.add(new ValidationException("Audit Record Type is null"), errors);
        }
        if (errors != null) {
            throw errors;
        }
    }
}