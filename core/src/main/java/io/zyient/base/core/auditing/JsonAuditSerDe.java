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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.CypherUtils;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.core.keystore.KeyStore;
import lombok.NonNull;
import org.apache.commons.lang3.SerializationException;

import java.nio.charset.StandardCharsets;

public class JsonAuditSerDe implements IAuditSerDe<String> {

    /**
     * Serialize the specified entity record.
     *
     * @param record - Entity record.
     * @return - Serialized Byte array.
     * @throws SerializationException
     */
    @Override
    public @NonNull String serialize(@NonNull Object record,
                                     @NonNull EncryptionInfo encryption,
                                     KeyStore keyStore) throws SerializationException {
        try {
            if (!encryption.encrypted())
                return JSONUtils.asString(record);
            else {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(encryption.password()));
                Preconditions.checkArgument(!Strings.isNullOrEmpty(encryption.iv()));
                Preconditions.checkArgument(keyStore != null);
                String password = keyStore.read(encryption.password());
                if (Strings.isNullOrEmpty(password)) {
                    throw new Exception(String.format("Encryption password not found. [name=%s]",
                            encryption.password()));
                }
                String json = JSONUtils.asString(record);
                return CypherUtils.encryptAsString(json, password, encryption.iv());
            }
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
    public <T> @NonNull T deserialize(@NonNull String data,
                                      @NonNull Class<? extends T> type,
                                      @NonNull EncryptionInfo encryption,
                                      KeyStore keyStore) throws SerializationException {
        try {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(data));
            if (encryption.encrypted()) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(encryption.password()));
                Preconditions.checkArgument(!Strings.isNullOrEmpty(encryption.iv()));
                Preconditions.checkArgument(keyStore != null);
                String password = keyStore.read(encryption.password());
                if (Strings.isNullOrEmpty(password)) {
                    throw new Exception(String.format("Encryption password not found. [name=%s]",
                            encryption.password()));
                }
                byte[] array = CypherUtils.decrypt(data, password, encryption.iv());
                if (array == null || array.length == 0) {
                    throw new Exception(String.format("Failed to decrypt string. [data=%s]", data));
                }
                data = new String(array, StandardCharsets.UTF_8);
            }
            T value = JSONUtils.read(data, type);
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
