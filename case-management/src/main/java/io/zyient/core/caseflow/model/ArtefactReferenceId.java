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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.core.persistence.model.DocumentId;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@Embeddable
public class ArtefactReferenceId implements IKey {
    @Embedded
    private CaseId caseId;
    @Embedded
    private DocumentId documentId;

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        try {
            return JSONUtils.asString(this);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
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
        if (key instanceof ArtefactReferenceId) {
            int ret = caseId.compareTo(((ArtefactReferenceId) key).caseId);
            if (ret == 0) {
                ret = documentId.compareTo(((ArtefactReferenceId) key).documentId);
            }
            return ret;
        }
        return 0;
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
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        ArtefactReferenceId id = JSONUtils.read(value, getClass());
        this.caseId = id.caseId;
        this.documentId = id.documentId;
        return this;
    }
}
