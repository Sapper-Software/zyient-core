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
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.Actor;
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
            @AttributeOverride(name = "type", column = @Column(name = "commented_by_type")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "comment_timestamp"))
    })
    private Actor commentedBy;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "caseId", column = @Column(name = "parent_comment_case_id")),
            @AttributeOverride(name = "commentId", column = @Column(name = "parent_comment_id"))
    })
    private CaseCommentId parent;
    @Enumerated(EnumType.STRING)
    @Column(name = "comment_state")
    private ECommentState commentState;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "collection", column = @Column(name = "artefact_collection")),
            @AttributeOverride(name = "id", column = @Column(name = "artefact_id"))
    })
    private DocumentId artefactId;
    @Column(name = "comment_text")
    private String comment;
    @Column(name = "reason_code")
    private String reasonCode;

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(CaseCommentId key) {
        if (key == null) {
            return Short.MAX_VALUE;
        }
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
    public IEntity<CaseCommentId> clone(Context context) throws CopyException {
        CaseCommentId cid = new CaseCommentId();
        cid.setCaseId(id.getCaseId());
        CaseComment c = new CaseComment();
        c.id = cid;
        c.comment = comment;
        c.artefactId = artefactId;
        c.commentedBy = commentedBy;
        c.parent = parent;
        c.commentState = commentState;
        c.getState().setState(EEntityState.New);
        return c;
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public CaseCommentId entityKey() {
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
        return errors;
    }
}
