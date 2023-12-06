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

package io.zyient.base.core.stores.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.IKey;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@Embeddable
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DocumentId implements IKey {
    @Column(name = "doc_id")
    private String id;
    @Column(name = "collection")
    private String collection;

    public DocumentId() {
        id = UUID.randomUUID().toString();
    }

    public DocumentId(@NonNull String collection,
                      @NonNull String id) {
        this.id = id;
        this.collection = collection;
    }

    public DocumentId(@NonNull String collection) {
        id = UUID.randomUUID().toString();
        this.collection = collection;
    }

    public DocumentId(@NonNull DocumentId id) {
        this.id = id.id;
        this.collection = id.collection;
    }

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        return String.format("%s" + __DEFAULT_SEPARATOR + "%s", collection, id);
    }

    /**
     * Compare the current key to the target.
     *
     * @param key - Key to compare to
     * @return - == 0, < -x, > +x
     */
    @Override
    public int compareTo(IKey key) {
        if (key == null) {
            return Short.MAX_VALUE;
        }
        if (key instanceof DocumentId) {
            return id.compareTo(((DocumentId) key).id);
        }
        throw new RuntimeException(String.format("Invalid document id. [type=%s]", key.getClass().getCanonicalName()));
    }

    /**
     * Parse this key type from the input string.
     *
     * @param value - Input key string.
     * @return - this
     * @throws Exception
     */
    @Override
    public IKey fromString(@NonNull String value) throws Exception {
        String[] parts = value.split(__DEFAULT_SEPARATOR);
        if (parts.length != 2) {
            throw new Exception(String.format("Invalid Document ID: [id=%s]", value));
        }
        String collection = parts[0].trim();
        String id = parts[1].trim();
        if (Strings.isNullOrEmpty(collection) || Strings.isNullOrEmpty(id)) {
            throw new Exception(String.format("Invalid Document ID: [id=%s]", value));
        }
        return this;
    }


    public static DocumentId parse(@NonNull String key) throws Exception {
        return (DocumentId) new DocumentId()
                .fromString(key);
    }
}