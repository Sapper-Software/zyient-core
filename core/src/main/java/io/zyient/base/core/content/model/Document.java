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

package io.zyient.base.core.content.model;

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
import io.zyient.base.core.model.BaseEntity;
import io.zyient.base.core.model.PropertyBag;
import io.zyient.base.core.model.UserContext;
import io.zyient.base.core.stores.impl.rdbms.converters.PropertiesConverter;
import io.zyient.base.core.stores.impl.solr.SolrConstants;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.solr.client.solrj.beans.Field;

import java.io.File;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

@Entity
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class Document<E extends Enum<?>, K extends IKey> extends BaseEntity<DocumentId> implements PropertyBag {
    @Transient
    @Field(SolrConstants.FIELD_SOLR_ID)
    private String searchId;
    @Transient
    @Field(SolrConstants.FIELD_REFERENCE_ID)
    private String searchReferenceId;
    @EmbeddedId
    private DocumentId id;
    @Column(name = "doc_name")
    @Field(SolrConstants.FIELD_SOLR_DOC_NAME)
    private String name;
    private DocumentState<E> docState;
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
    private K referenceId;
    @Transient
    @JsonIgnore
    private File path;
    @Convert(converter = PropertiesConverter.class)
    @Field(SolrConstants.FIELD_DOC_PROPERTIES)
    private Map<String, Object> properties;

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
        if (!(source instanceof Document<?, ?>)) {
            throw new CopyException(String.format("Invalid source type. [type=%s]",
                    source.getClass().getCanonicalName()));
        }
        super.copyChanges(source, context);
        Document<E, K> doc = (Document<E, K>) source;
        this.docState = doc.docState;
        this.name = doc.name;
        this.uri = doc.uri;
        this.mimeType = doc.mimeType;
        this.createdBy = doc.createdBy;
        this.modifiedBy = doc.modifiedBy;
        this.referenceId = doc.referenceId;
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
            Document<E, K> doc = getClass().getDeclaredConstructor()
                    .newInstance();
            doc.id = new DocumentId();
            clone(doc, EEntityState.New);
            doc.docState = docState.create();
            doc.name = name;
            doc.uri = uri;
            doc.mimeType = mimeType;
            if (context instanceof UserContext) {
                Principal p = ((UserContext) context).user();
                doc.createdBy = p.getName();
                doc.modifiedBy = p.getName();
            } else {
                doc.modifiedBy = modifiedBy;
                doc.createdBy = createdBy;
            }
            doc.referenceId = referenceId;
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
    public ValidationExceptions doValidate(ValidationExceptions errors) throws ValidationExceptions {
        if (Strings.isNullOrEmpty(uri)) {
            errors = ValidationExceptions.add(new ValidationException("Missing required field: [uri]"), errors);
        }
        if (Strings.isNullOrEmpty(createdBy) || Strings.isNullOrEmpty(modifiedBy)) {
            errors = ValidationExceptions.add(new ValidationException("Missing required field(s) : [createdBy/modifiedBy]"), errors);
        }
        if (referenceId != null) {
            Class<? extends IKey> type = referenceId.getClass();
            if (!type.isAnnotationPresent(Embeddable.class)) {
                errors = ValidationExceptions.add(new ValidationException(
                        String.format("Invalid reference ID: [type=%s]", type.getCanonicalName())), errors);
            }
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
}
