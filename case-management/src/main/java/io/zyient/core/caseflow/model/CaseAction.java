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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.StringKey;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@Entity
@Table(name = "cm_case_actions")
public class CaseAction implements IEntity<StringKey> {
    public static final int __START_INDEX = 1000;
    public static final int __KEY_SIZE = 16;
    public static final String __KEY_FORMAT = "%" + __KEY_SIZE + "d";

    @EmbeddedId
    private StringKey key;
    @Column(name = "description")
    private String description;

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(StringKey key) {
        return this.key.compareTo(key);
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
    public IEntity<StringKey> copyChanges(IEntity<StringKey> source,
                                          Context context) throws CopyException {
        if (source instanceof CaseAction) {
            description = ((CaseAction) source).description;
        } else {
            throw new CopyException(String.format("Invalid source object. [type=%s]",
                    source.getClass().getCanonicalName()));
        }
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
    public IEntity<StringKey> clone(Context context) throws CopyException {
        throw new CopyException("Not implemented...");
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public StringKey entityKey() {
        return key;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        ValidationExceptions errors = null;
        if (key == null || Strings.isNullOrEmpty(key.getKey())) {
            errors = ValidationExceptions.add(new ValidationException("Key is NULL/Empty."), errors);
        }
        try {
            int ii = extractIntId(key.getKey());
            if (ii <= __START_INDEX) {
                errors = ValidationExceptions.add(
                        new ValidationException(String.format("Invalid key index: must be greater than %d. [index=%d]",
                                __START_INDEX, ii)), errors);
            } else if (key.getKey().length() < __KEY_SIZE) {
                errors = ValidationExceptions.add(
                        new ValidationException(String.format("Invalid Key size. [size=%d]",
                                key.getKey().length())), errors);
            }
        } catch (NumberFormatException nfe) {
            errors = ValidationExceptions.add(
                    new ValidationException(String.format("Invalid key format. [key=%s]", key.getKey())), errors);
        }
        if (Strings.isNullOrEmpty(description)) {
            errors = ValidationExceptions.add(new ValidationException("Description is NULL/Empty."), errors);
        }
        if (errors != null) {
            throw errors;
        }
    }

    public static String formatActionId(int id) {
        Preconditions.checkArgument(id > 0);
        return String.format(__KEY_FORMAT, id);
    }

    public static int extractIntId(@NonNull String id) {
        return Integer.parseInt(id);
    }
}
