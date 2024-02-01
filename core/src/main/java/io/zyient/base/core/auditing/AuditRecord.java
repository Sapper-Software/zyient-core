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

package io.zyient.base.core.auditing;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.StringKey;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.sql.Timestamp;
import java.util.UUID;

@Getter
@Setter
@MappedSuperclass
public abstract class AuditRecord<R> implements IEntity<StringKey> {
    @EmbeddedId
    private StringKey id;
    @Enumerated(EnumType.STRING)
    @Column(name = "audit_record_type")
    private AuditRecordType type;
    @Column(name = "namespace")
    private String namespace;
    @Column(name = "module")
    private String module;
    @Column(name = "instance_id")
    private String instanceId;
    @Column(name = "db_timestamp")
    private Timestamp dbTimestamp;
    @Embedded
    private ActionedBy actor;
    @Column(name = "data_type")
    private String recordType;


    protected AuditRecord() {
        id = new StringKey(UUID.randomUUID().toString());
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(StringKey key) {
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
    public IEntity<StringKey> copyChanges(IEntity<StringKey> source, Context context) throws CopyException {
        throw new CopyException("Method not implemented...");
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<StringKey> clone(Context context) throws CopyException {
        throw new CopyException("Method not implemented...");
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public StringKey entityKey() {
        return id;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        errors = ValidationExceptions.checkCondition(actor != null,
                "Audit user not specified.",
                errors);
        if (actor != null) {
            errors = ValidationExceptions.checkValue(actor.getName(),
                    "Invalid Actor: User/Role name is NULL",
                    errors);
        }
        if (errors != null) {
            throw errors;
        }
        if (actor.getTimestamp() <= 0) {
            actor.setTimestamp(System.currentTimeMillis());
        }
        dbTimestamp = new Timestamp(actor.getTimestamp());
    }

    public abstract void data(@NonNull R data);

    public abstract R data();
}
