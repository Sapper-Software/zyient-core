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
import io.zyient.base.core.model.PropertyBag;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.core.persistence.impl.rdbms.converters.PropertiesConverter;
import io.zyient.core.persistence.model.BaseEntity;
import io.zyient.core.persistence.model.Document;
import io.zyient.core.persistence.model.DocumentId;
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
public abstract class Case<S extends CaseState<?>> extends BaseEntity<CaseId> implements PropertyBag {
    @EmbeddedId
    private CaseId id;
    @Embedded
    private S state;
    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "name", column = @Column(name = "assigned_to")),
            @AttributeOverride(name = "type", column = @Column(name = "assigned_type")),
            @AttributeOverride(name = "timestamp", column = @Column(name = "assigned_timestamp"))
    })
    private UserOrRole assignedTo;
    @OneToMany
    private Set<CaseComment> comments;
    @OneToMany
    private Set<ArtefactReference> artefactReferences;
    @Transient
    private Set<Document<?, ?, ?>> artefacts;
    @Column(name = "properties")
    @Convert(converter = PropertiesConverter.class)
    private Map<String, Object> properties;
    @Transient
    private Set<ArtefactReference> deleted;

    public Case() {
        id = new CaseId();
    }

    public Case<S> addArtefact(@NonNull Document<?, ?, ?> artefact) {
        ArtefactReferenceId refId = new ArtefactReferenceId();
        refId.setCaseId(id);
        refId.setDocumentId(artefact.getId());
        ArtefactReference reference = new ArtefactReference();
        reference.setId(refId);
        reference.setArtefactType(artefact.getClass().getCanonicalName());
        artefactReferences.add(reference);
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
            deleted.add(delete);
            Document<?, ?, ?> deleteDoc = null;
            for (Document<?, ?, ?> document : artefacts) {
                if (document.getId().compareTo(delete.getId().getDocumentId()) == 0) {
                    deleteDoc = document;
                    break;
                }
            }
            if (deleteDoc != null) {
                artefacts.remove(deleteDoc);
            }
            return true;
        }
        return false;
    }

    public Case<S> addComment(@NonNull UserOrRole commentor,
                              @NonNull String comment,
                              Long responseTo) throws DataStoreException {

        return this;
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
}
