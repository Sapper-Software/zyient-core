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

import io.zyient.base.core.keystore.KeyStore;
import lombok.NonNull;
import org.apache.commons.lang3.SerializationException;

/**
 * Interface to implement entity record serializer/de-serializer.
 */
public interface IAuditSerDe<R> {

    /**
     * Serialize the specified entity record.
     *
     * @param record - Entity record.
     * @return - Serialized Byte array.
     * @throws SerializationException
     */
    @NonNull
    R serialize(@NonNull Object record,
                @NonNull EncryptionInfo encryption,
                KeyStore keyStore) throws SerializationException;

    /**
     * Read the entity record from the byte array passed.
     *
     * @param data - Input Byte data.
     * @param type - Entity type being serialized.
     * @return - De-serialized entity record.
     * @throws SerializationException
     */
    @NonNull <T> T deserialize(@NonNull R data, @NonNull Class<? extends T> type,
                               @NonNull EncryptionInfo encryption,
                               KeyStore keyStore) throws SerializationException;
}
