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
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.JSONUtils;
import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class CaseHistoryId implements IKey {
    @Column(name = "case_id")
    private String caseId;
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "sequence")
    private int sequence;

    /**
     * Get the String representation of the key.
     *
     * @return - Key String
     */
    @Override
    public String stringKey() {
        try {
            return JSONUtils.asString(this, getClass());
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
        if (key instanceof CaseHistoryId) {
            int ret = caseId.compareTo(((CaseHistoryId) key).caseId);
            if (ret == 0) {
                ret = sequence - ((CaseHistoryId) key).sequence;
            }
            return ret;
        }
        throw new RuntimeException(String.format("Invalid Comment ID: [type=%s]", key.getClass().getCanonicalName()));
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
        CaseHistoryId id = JSONUtils.read(value, getClass());
        caseId = id.caseId;
        sequence = id.sequence;
        return this;
    }
}
