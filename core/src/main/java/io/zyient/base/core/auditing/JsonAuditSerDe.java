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

package io.zyient.base.core.auditing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.GlobalConstants;
import lombok.NonNull;
import org.apache.commons.lang3.SerializationException;

public class JsonAuditSerDe implements IAuditSerDe {

    /**
     * Serialize the specified entity record.
     *
     * @param record - Entity record.
     * @param type   - Entity type being serialized.
     * @return - Serialized Byte array.
     * @throws SerializationException
     */
    @NonNull
    @Override
    public String serialize(@NonNull Object record, @NonNull Class<?> type) throws SerializationException {
        try {
            ObjectMapper mapper = GlobalConstants.getJsonMapper();
            String json = mapper.writeValueAsString(record);
            if (Strings.isNullOrEmpty(json)) {
                throw new SerializationException(String.format("Error serializing record. [type=%s]",
                        type.getCanonicalName()));
            }
            return json;
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }

    /**
     * Read the entity record from the byte array passed.
     *
     * @param data - Input Byte data.
     * @param type - Entity type being serialized.
     * @return - De-serialized entity record.
     * @throws SerializationException
     */
    @Override
    public <T> @NonNull T deserialize(@NonNull String data, @NonNull Class<? extends T> type) throws SerializationException {
        try {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(data));
            ObjectMapper mapper = GlobalConstants.getJsonMapper();
            T value = mapper.readValue(data, type);
            if (value == null) {
                throw new SerializationException(String.format("Error de-serializing record. [type=%s]",
                        type.getCanonicalName()));
            }
            return value;
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }
}
