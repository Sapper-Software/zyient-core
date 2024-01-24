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

package io.zyient.core.persistence.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.model.entity.PropertyBag;
import io.zyient.base.core.model.UserContext;
import io.zyient.core.persistence.impl.rdbms.converters.PropertiesConverter;
import io.zyient.core.persistence.impl.solr.SolrConstants;
import io.zyient.core.sdk.model.content.Content;
import io.zyient.core.sdk.model.content.ContentId;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.solr.client.solrj.beans.Field;

import java.io.File;
import java.security.Principal;
import java.util.*;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@MappedSuperclass
public abstract class Document<E extends DocumentState<?>, K extends IKey, T extends Document<E, K, T>>
        extends BaseEntity<DocumentId> implements PropertyBag {
    public static final String PROPERTY_PATH_MAP = "__fs_path_info";

    @Transient
    @Field(SolrConstants.FIELD_SOLR_ID)
    private String searchId;
    @Transient
    @Field(SolrConstants.FIELD_REFERENCE_ID)
    private String searchReferenceId;

    @EmbeddedId
    private DocumentId id;
    @Column(name = "parent_doc_id")
    @Field(SolrConstants.FIELD_DOC_PARENT_ID)
    private String parentDocId;
    @Column(name = "doc_name")
    @Field(SolrConstants.FIELD_SOLR_DOC_NAME)
    private String name;
    @Column(name = "doc_source_path")
    @Field(SolrConstants.FIELD_SOLR_DOC_SOURCE_PATH)
    private String sourcePath;
    @Embedded
    private E docState;
    @Column(name = "mime_type")
    @Field(SolrConstants.FIELD_SOLR_MIME_TYPE)
    private String mimeType;
    @Field(SolrConstants.FIELD_SOLR_URI)
    @Column(name = "URI")
    private String uri;
    @Column(name = "created_by")
    @Field(SolrConstants.FIELD_SOLR_CREATED_BY)
    private String createdBy;
    @Column(name = "modified_by")
    @Field(SolrConstants.FIELD_SOLR_MODIFIED_BY)
    private String modifiedBy;
    @Embedded
    private K referenceId;
    @Column(name = "passkey")
    private String password;
    @Transient
    @JsonIgnore
    private File path;
    @Transient
    @JsonIgnore
    private Set<T> documents;
    @Column(name = "document_count")
    @Field(SolrConstants.FIELD_DOCUMENT_COUNT)
    private int documentCount = 0;
    @Convert(converter = PropertiesConverter.class)
    @Field(SolrConstants.FIELD_DOC_PROPERTIES)
    @Column(name = "properties")
    private Map<String, Object> properties;

    protected Document(@NonNull E docState) {
        this.docState = docState;
    }

    /**
     * Compare the entity key with the key specified.
     *
     * @param key - Target Key.
     * @return - Comparision.
     */
    @Override
    public int compare(DocumentId key) {
        return id.compareTo(key);
    }

    @SuppressWarnings("unchecked")
    public Document<E, K, T> add(@NonNull Document<?, ?, ?> doc) {
        if (documents == null) {
            documents = new HashSet<>();
        }
        documents.add((T) doc);
        documentCount++;
        return this;
    }

    @SuppressWarnings("unchecked")
    public boolean remove(@NonNull Document<?, ?, ?> doc) {
        if (documents != null) {
            return documents.remove((T) doc);
        }
        documentCount--;
        return false;
    }

    @SuppressWarnings("unchecked")
    public Document<E, K, T> addAll(@NonNull Collection<Document<E, K, T>> docs) {
        if (documents == null) {
            documents = new HashSet<>();
        }
        documents.addAll((Collection<? extends T>) docs);
        return this;
    }

    @SuppressWarnings("unchecked")
    public boolean remove(@NonNull DocumentId key) {
        if (documents != null) {
            Document<E, K, T> remove = null;
            for (Document<E, K, T> doc : documents) {
                if (doc.getId().compareTo(key) == 0) {
                    remove = doc;
                    break;
                }
            }
            if (remove != null) {
                return documents.remove((T) remove);
            }
        }
        return false;
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
    public IEntity<DocumentId> copyChanges(IEntity<DocumentId> source, Context context) throws CopyException {
        if (!(source instanceof Document<?, ?, ?>)) {
            throw new CopyException(String.format("Invalid source type. [type=%s]",
                    source.getClass().getCanonicalName()));
        }
        super.copyChanges(source, context);
        Document<E, K, T> doc = (Document<E, K, T>) source;
        this.docState = doc.docState;
        this.name = doc.name;
        this.sourcePath = doc.sourcePath;
        this.uri = doc.uri;
        this.mimeType = doc.mimeType;
        this.createdBy = doc.createdBy;
        this.modifiedBy = doc.modifiedBy;
        this.parentDocId = doc.parentDocId;
        this.referenceId = doc.referenceId;
        if (doc.documents != null) {
            this.documents = new HashSet<>(doc.documents);
        }
        this.properties = doc.properties;
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
    @SuppressWarnings("unchecked")
    public IEntity<DocumentId> clone(Context context) throws CopyException {
        try {
            Document<E, K, T> doc = getClass().getDeclaredConstructor()
                    .newInstance();
            doc.id = new DocumentId();
            clone(doc, EEntityState.New);
            doc.docState = (E) docState.create();
            doc.name = name;
            doc.sourcePath = sourcePath;
            doc.uri = uri;
            doc.mimeType = mimeType;
            doc.parentDocId = parentDocId;
            if (context instanceof UserContext) {
                Principal p = ((UserContext) context).user();
                doc.createdBy = p.getName();
                doc.modifiedBy = p.getName();
            } else {
                doc.modifiedBy = modifiedBy;
                doc.createdBy = createdBy;
            }
            if (documents != null) {
                doc.documents = new HashSet<>(documents.size());
                for (T d : documents) {
                    doc.documents.add((T) d.clone(context));
                }
            }
            doc.referenceId = referenceId;
            if (properties != null) {
                doc.properties = new HashMap<>(properties);
            }
            return this;
        } catch (Exception ex) {
            throw new CopyException(ex);
        }
    }

    /**
     * Get the object instance Key.
     *
     * @return - Key
     */
    @Override
    public DocumentId entityKey() {
        return id;
    }

    /**
     * Validate this entity instance.
     *
     * @param errors
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    @SuppressWarnings("unchecked")
    public ValidationExceptions doValidate(ValidationExceptions errors) throws ValidationExceptions {
        if (Strings.isNullOrEmpty(uri)) {
            errors = ValidationExceptions.add(new ValidationException("Missing required field: [uri]"), errors);
        }
        if (Strings.isNullOrEmpty(createdBy) || Strings.isNullOrEmpty(modifiedBy)) {
            errors = ValidationExceptions
                    .add(new ValidationException("Missing required field(s) : [createdBy/modifiedBy]"), errors);
        }
        if (documentCount > 0) {
            if (documents == null) {
                errors = ValidationExceptions
                        .add(new ValidationException(String.format("No nested documents. [expected count=%d]",
                                documentCount)), errors);
            } else if (documents.size() != documentCount) {
                errors = ValidationExceptions
                        .add(new ValidationException(String.format("No nested documents. [expected count=%d][actual=%d]",
                                documentCount, documents.size())), errors);
            }
        }
        if (referenceId != null) {
            Class<? extends IKey> type = referenceId.getClass();
            if (!type.isAnnotationPresent(Embeddable.class)) {
                errors = ValidationExceptions.add(new ValidationException(
                        String.format("Invalid reference ID: [type=%s]", type.getCanonicalName())), errors);
            }
        }
        Map<String, String> pathConfig = (Map<String, String>) getProperty(PROPERTY_PATH_MAP);
        if (pathConfig == null || pathConfig.isEmpty()) {
            errors = ValidationExceptions.add(new ValidationException("Document path config property not set."), errors);
        }
        return errors;
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
    public PropertyBag setProperty(@NonNull String name,
                                   @NonNull Object value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(name, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> pathConfig() throws Exception {
        Map<String, String> map = (Map<String, String>) getProperty(Document.PROPERTY_PATH_MAP);
        if (map == null) {
            throw new Exception(String.format("Invalid document: path config property not found. [id=%s]",
                    getId().stringKey()));
        }
        return map;
    }

    public Content as(@NonNull Content content) {
        ContentId id = new ContentId();
        id.setCollection(this.id.getCollection());
        id.setId(this.id.getId());
        content.setId(id);
        content.setName(name);
        content.setCreatedBy(createdBy);
        content.setProperties(properties);
        content.setModifiedBy(modifiedBy);
        content.setMimeType(mimeType);
        content.setURI(uri);
        content.setState(docState.getState().name());
        if (docState.hasError()) {
            Throwable t = docState.getError();
            content.setError(t.getLocalizedMessage());
        }
        content.setTimeCreated(getCreatedTime());
        content.setTimeModified(getUpdatedTime());
        content.setNestedContentCount(documentCount);
        return content;
    }

    public abstract Content as();

    public abstract Document<E, K, T> createInstance() throws Exception;
}
