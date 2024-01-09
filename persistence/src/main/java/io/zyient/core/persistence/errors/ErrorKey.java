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

package io.zyient.core.persistence.errors;

import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@Embeddable
public class ErrorKey implements IKey {
    @Column(name = "error_type")
    private String type;
    @Column(name = "error_code")
    private int errorCode;

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
            DefaultLogger.stacktrace(ex);
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
        if (key instanceof ErrorKey) {
            int ret = type.compareTo(((ErrorKey) key).type);
            if (ret == 0) {
                ret = errorCode - ((ErrorKey) key).errorCode;
            }
            return ret;
        }
        throw new RuntimeException(String.format("Invalid key type. [type=%s]", key.getClass().getCanonicalName()));
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
        ErrorKey k = JSONUtils.read(value, getClass());
        type = k.type;
        errorCode = k.errorCode;
        return this;
    }
}
