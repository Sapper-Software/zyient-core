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

package io.zyient.core.caseflow.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.Actor;
import io.zyient.core.persistence.model.BaseEntity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.ColumnTransformer;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@Entity
@Table(name = "cm_case_history")
public class CaseHistory extends BaseEntity<CaseHistoryId> {
    @EmbeddedId
    private CaseHistoryId id;
    @Column(name = "action_id")
    private String action;
    @Column(name = "code_id")
    private String caseCode;
    @Column(name = "comment")
    private String comment;
    @Column(name = "change_json", columnDefinition = "json")
    @ColumnTransformer(write = "?::json")
    private String change;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "name", column = @Column(name = "created_by")),
            @AttributeOverride(name = "type", column = @Column(name = "created_by_type")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "created_timestamp"))
    })
    private Actor actor;

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(CaseHistoryId key) {
        return id.compareTo(key);
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<CaseHistoryId> clone(Context context) throws CopyException {
        throw new CopyException("Not implemented...");
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public CaseHistoryId entityKey() {
        return id;
    }

    /**
     * Validate this entity instance.
     *
     * @param errors
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public ValidationExceptions doValidate(ValidationExceptions errors) throws ValidationExceptions {
        if (Strings.isNullOrEmpty(action)) {
            errors = ValidationExceptions.add(new ValidationException("Action code is NULL/empty"), errors);
        }
        if (Strings.isNullOrEmpty(caseCode)) {
            errors = ValidationExceptions.add(new ValidationException("Case code is NULL/empty"), errors);
        }
        if (Strings.isNullOrEmpty(comment)) {
            errors = ValidationExceptions.add(new ValidationException("Comment is NULL/empty."), errors);
        }
        return errors;
    }
}
