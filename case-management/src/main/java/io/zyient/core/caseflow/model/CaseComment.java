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
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.core.persistence.model.BaseEntity;
import io.zyient.core.persistence.model.DocumentId;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@Entity
@Table(name = "cm_case_comments")
public class CaseComment extends BaseEntity<CaseCommentId> {
    @EmbeddedId
    private CaseCommentId id;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "name", column = @Column(name = "commented_by")),
            @AttributeOverride(name = "type", column = @Column(name = "commentor_type")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "comment_timestamp"))
    })
    private UserOrRole commentedBy;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "case_id", column = @Column(name = "parent_case_id")),
            @AttributeOverride(name = "comment_id", column = @Column(name = "parent_comment_id"))
    })
    private CaseCommentId parent;
    @Enumerated(EnumType.STRING)
    @Column(name = "comment_state")
    private ECaseCommentState state;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "collection", column = @Column(name = "artefact_collection")),
            @AttributeOverride(name = "id", column = @Column(name = "artefact_id"))
    })
    private DocumentId artefactId;

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(CaseCommentId key) {
        return 0;
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<CaseCommentId> clone(Context context) throws CopyException {
        return null;
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public CaseCommentId entityKey() {
        return null;
    }

    /**
     * Validate this entity instance.
     *
     * @param errors
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public ValidationExceptions doValidate(ValidationExceptions errors) throws ValidationExceptions {
        return null;
    }
}
