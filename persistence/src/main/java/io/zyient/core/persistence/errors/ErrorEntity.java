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

package io.zyient.core.persistence.errors;

import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.errors.Error;
import io.zyient.core.persistence.model.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "common_errors")
public class ErrorEntity extends BaseEntity<ErrorKey> {
    @EmbeddedId
    private ErrorKey key;
    @Column(name = "message")
    private String message;
    @Column(name = "deleted")
    private boolean deleted;

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(ErrorKey key) {
        return this.key.compareTo(key);
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<ErrorKey> clone(Context context) throws CopyException {
        throw new CopyException("Method not implemented...");
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public ErrorKey entityKey() {
        return key;
    }

    /**
     * Validate this entity instance.
     *
     * @param errors
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public ValidationExceptions doValidate(ValidationExceptions errors) throws ValidationExceptions {
        if (key == null) {
            errors = ValidationExceptions.add(new ValidationException("Entity key is null..."), errors);
        } else if (Strings.isNullOrEmpty(key.getType())) {
            errors = ValidationExceptions.add(new ValidationException("Error type is NULL/empty..."), errors);
        } else if (key.getErrorCode() <= 0) {
            errors = ValidationExceptions.add(new ValidationException(
                    String.format("Invalid error code. [%d]", key.getErrorCode())), errors);
        }
        if (Strings.isNullOrEmpty(message)) {
            errors = ValidationExceptions.add(new ValidationException("Message is NULL/empty..."), errors);
        }
        return errors;
    }

    public Error to() {
        Error error = new Error();
        error.setType(key.getType());
        error.setErrorCode(key.getErrorCode());
        error.setMessage(message);
        return error;
    }
}
