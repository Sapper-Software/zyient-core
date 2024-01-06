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
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.Actor;
import io.zyient.base.core.model.EUserOrRole;
import io.zyient.base.core.model.PropertyBag;
import io.zyient.base.core.model.UserOrRole;
import io.zyient.core.caseflow.errors.CaseModelException;
import io.zyient.core.persistence.impl.rdbms.converters.PropertiesConverter;
import io.zyient.core.persistence.model.BaseEntity;
import io.zyient.core.persistence.model.DocumentId;
import io.zyient.core.persistence.model.DocumentState;
import io.zyient.core.sdk.model.caseflow.CaseEntity;
import io.zyient.core.sdk.model.content.Content;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
@MappedSuperclass
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class Case<P extends Enum<P>, S extends CaseState<P>, E extends DocumentState<?>, T extends CaseDocument<E, T>>
        extends BaseEntity<CaseId> implements PropertyBag {
    @EmbeddedId
    private CaseId id;
    @Column(name = "case_name")
    private String name;
    @Column(name = "description")
    private String description;
    @Embedded
    private S caseState;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "name", column = @Column(name = "created_by")),
            @AttributeOverride(name = "type", column = @Column(name = "created_by_type")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "created_timestamp"))
    })
    private Actor createdBy;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "name", column = @Column(name = "assigned_to")),
            @AttributeOverride(name = "type", column = @Column(name = "assigned_to_type")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "assigned_timestamp"))
    })
    private Actor assignedTo;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "name", column = @Column(name = "closed_by")),
            @AttributeOverride(name = "type", column = @Column(name = "closed_by_type")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "closed_timestamp"))
    })
    private Actor closedBy;
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "case_id", referencedColumnName = "case_id")
    private Set<CaseComment> comments;
    @OneToMany(cascade = CascadeType.ALL)
    @JoinColumn(name = "case_id", referencedColumnName = "case_id")
    private Set<ArtefactReference> artefactReferences;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "id", column = @Column(name = "parent_case_id"))
    })
    private CaseId parentId;
    @Transient
    private Set<CaseDocument<E, T>> artefacts;
    @Column(name = "properties")
    @Convert(converter = PropertiesConverter.class)
    private Map<String, Object> properties;
    @Transient
    private Set<CaseDocument<E, T>> deleted;
    @Column(name = "external_reference_id")
    private String externalReferenceId;

    public Case() {
        id = new CaseId();
    }

    public Case<P, S, E, T> addArtefact(@NonNull CaseDocument<E, T> artefact) {
        ArtefactReferenceId refId = new ArtefactReferenceId();
        refId.setCaseId(id);
        refId.setDocumentId(artefact.getId());
        ArtefactReference reference = new ArtefactReference();
        reference.setId(refId);
        reference.setArtefactType(artefact.getClass().getCanonicalName());
        if (artefactReferences == null) {
            artefactReferences = new HashSet<>();
        }
        artefactReferences.add(reference);
        artefact.setReferenceId(id);
        if (artefacts == null) {
            artefacts = new HashSet<>();
        }
        artefacts.add(artefact);

        return this;
    }

    public boolean deleteArtefact(@NonNull DocumentId docId) {
        ArtefactReference delete = null;
        for (ArtefactReference reference : artefactReferences) {
            if (reference.getId().getDocumentId().compareTo(docId) == 0) {
                delete = reference;
                break;
            }
        }
        if (delete != null) {
            artefactReferences.remove(delete);
            if (deleted == null) {
                deleted = new HashSet<>();
            }
            CaseDocument<E, T> deleteDoc = null;
            for (CaseDocument<E, T> document : artefacts) {
                if (document.getId().compareTo(delete.getId().getDocumentId()) == 0) {
                    deleteDoc = document;
                    break;
                }
            }
            if (deleteDoc != null) {
                artefacts.remove(deleteDoc);
                deleted.add(deleteDoc);
            }
            return true;
        }
        return false;
    }

    public CaseDocument<E, T> findArtefact(@NonNull DocumentId documentId) {
        if (artefacts != null) {
            for (CaseDocument<E, T> document : artefacts) {
                if (document.getId().compareTo(documentId) == 0) {
                    return document;
                }
            }
        }
        return null;
    }

    public CaseComment addComment(@NonNull String userOrRole,
                                  @NonNull EUserOrRole type,
                                  @NonNull String comment,
                                  @NonNull CaseCode reason,
                                  Long responseTo,
                                  ECommentState responseState,
                                  DocumentId documentId) throws CaseModelException {
        Actor ur = new Actor();
        ur.setName(userOrRole);
        ur.setType(type);
        ur.setTimestamp(System.currentTimeMillis());
        return addComment(ur, comment, reason, responseTo, responseState, documentId);
    }

    public CaseComment addComment(@NonNull UserOrRole actor,
                                  @NonNull String comment,
                                  @NonNull CaseCode reason,
                                  Long responseTo,
                                  ECommentState responseState,
                                  DocumentId docId) throws CaseModelException {
        CaseComment parent = null;
        if (responseTo != null) {
            parent = findParent(responseTo);
            if (parent == null) {
                throw new CaseModelException(String.format("Parent comment not found. [case=%s][comment id=%d]",
                        id.stringKey(), responseTo));
            }
            parent.setCommentState(responseState);
        }
        Actor a = null;
        if (actor instanceof Actor) {
            a = (Actor) actor;
        } else {
            a = new Actor(actor);
        }
        a.setTimestamp(System.currentTimeMillis());
        CaseCommentId id = new CaseCommentId();
        id.setCaseId(this.id.getId());
        CaseComment c = new CaseComment();
        c.setId(id);
        c.setCommentedBy(a);
        c.setComment(comment);
        c.setReasonCode(reason.getKey().getKey());
        if (parent != null)
            c.setParent(parent.getId());
        c.setCommentState(ECommentState.New);
        if (docId != null) {
            CaseDocument<E, T> artefact = findArtefact(docId);
            if (artefact == null) {
                throw new CaseModelException(String.format("Artefact not found. [case=%s][artefact id=%s]",
                        id.getCaseId(), docId.stringKey()));
            }
            c.setArtefactId(docId);
        }
        comments.add(c);

        return c;
    }

    private CaseComment findParent(long parentId) {
        if (comments != null) {
            for (CaseComment comment : comments) {
                if (comment.getId().getCommentId() == parentId) {
                    return comment;
                }
            }
        }
        return null;
    }

    @Override
    public boolean hasProperty(@NonNull String name) {
        if (properties != null) {
            return properties.containsKey(name);
        }
        return false;
    }

    @Override
    public Object getProperty(@NonNull String name) {
        if (properties != null) {
            return properties.get(name);
        }
        return null;
    }

    @Override
    public PropertyBag setProperty(@NonNull String name, @NonNull Object value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(name, value);
        return this;
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(CaseId key) {
        return id.compareTo(key);
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public CaseId entityKey() {
        return id;
    }

    /**
     * Clone this instance of Entity.
     *
     * @param context - Clone Context.
     * @return - Cloned Instance.
     * @throws CopyException
     */
    @Override
    public IEntity<CaseId> clone(Context context) throws CopyException {
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
        if (id == null) {
            errors = ValidationExceptions.add(new ValidationException("Case ID not set."), errors);
        }
        if (Strings.isNullOrEmpty(id.getId())) {
            errors = ValidationExceptions.add(new ValidationException("Case ID is null/empty."), errors);
        }
        if (Strings.isNullOrEmpty(description)) {
            errors = ValidationExceptions.add(new ValidationException("Case description is null/empty"), errors);
        }
        if (createdBy == null) {
            errors = ValidationExceptions.add(new ValidationException("Created By not set."), errors);
        }
        return errors;
    }

    public abstract CaseEntity<P> as();

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
    public IEntity<CaseId> copyChanges(IEntity<CaseId> source, Context context) throws CopyException {
        return super.copyChanges(source, context);
    }

    @Override
    public void clone(@NonNull BaseEntity<CaseId> entity,
                      @NonNull EEntityState state) throws CopyException {
        super.clone(entity, state);
    }

    protected CaseEntity<P> as(@NonNull CaseEntity<P> caseEntity) {
        caseEntity.setCaseId(id.getId());
        caseEntity.setCaseState(caseState.getState());
        caseEntity.setDescription(description);
        if(parentId!=null) {
            caseEntity.setParentCaseId(parentId.getId());
        }
        caseEntity.setAssignedTo(assignedTo);
        caseEntity.setCreatedBy(createdBy);
        caseEntity.setProperties(properties);
        caseEntity.setClosedBy(closedBy);
        caseEntity.setExternalReferenceId(externalReferenceId);
        caseEntity.setName(name);
        if (artefacts != null) {
            Set<Content> contents = new HashSet<>(artefacts.size());
            for (CaseDocument<E, T> doc : artefacts) {
                Content c = doc.as();
                contents.add(c);
            }
            if (!contents.isEmpty()) {
                caseEntity.setDocuments(contents);
            }
        }
        return caseEntity;
    }
}
