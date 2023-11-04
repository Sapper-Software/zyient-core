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

package io.zyient.base.core.stores.impl.solr;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.StringKey;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.solr.client.solrj.beans.Field;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SolrDocumentEntity extends SolrEntity<StringKey> {
    @Field
    private StringKey key;
    private File content;
    @Field
    private String sourceLocation;
    @Field
    private String mimeType;
    @Field
    private long size;

    public SolrDocumentEntity() {
        key = new StringKey(UUID.randomUUID().toString());
    }

    public SolrDocumentEntity(String source,
                              @NonNull String file) throws Exception {
        key = new StringKey(UUID.randomUUID().toString());
        content = new File(file);
        if (!content.exists()) {
            throw new IOException(String.format("Source file not found. [path=%s]", content.getAbsolutePath()));
        }
        size = content.length();
        sourceLocation = source;
    }

    public SolrDocumentEntity(String source,
                              @NonNull File file) throws Exception {
        key = new StringKey(UUID.randomUUID().toString());
        content = file;
        if (!content.exists()) {
            throw new IOException(String.format("Source file not found. [path=%s]", content.getAbsolutePath()));
        }
        size = content.length();
        sourceLocation = source;
    }

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
        Preconditions.checkArgument(source instanceof SolrDocumentEntity);
        super.copyChanges(source, context);
        content = ((SolrDocumentEntity) source).content;
        key = ((SolrDocumentEntity) source).key;
        mimeType = ((SolrDocumentEntity) source).mimeType;
        size = ((SolrDocumentEntity) source).size;
        sourceLocation = ((SolrDocumentEntity) source).sourceLocation;

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
        try {
            SolrDocumentEntity entity = new SolrDocumentEntity();
            entity.setContent(content);
            entity.setMimeType(mimeType);
            entity.setSize(size);
            entity.setSourceLocation(sourceLocation);
            entity.validate();
            return entity;
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
    public StringKey entityKey() {
        return key;
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void doValidate() throws ValidationExceptions {

    }

}
