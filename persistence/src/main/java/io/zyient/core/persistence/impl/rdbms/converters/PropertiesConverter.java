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

package io.zyient.core.persistence.impl.rdbms.converters;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import jakarta.persistence.AttributeConverter;

import java.util.Map;

public class PropertiesConverter implements AttributeConverter<Map<String, Object>, String> {
    /**
     * Converts the value stored in the entity attribute into the
     * data representation to be stored in the database.
     *
     * @param attribute the entity attribute value to be converted
     * @return the converted data to be stored in the database
     * column
     */
    @Override
    public String convertToDatabaseColumn(Map<String, Object> attribute) {
        try {
            if (attribute != null) {
                return JSONUtils.asString(attribute);
            }
            return null;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Converts the data stored in the database column into the
     * value to be stored in the entity attribute.
     * Note that it is the responsibility of the converter writer to
     * specify the correct <code>dbData</code> type for the corresponding
     * column for use by the JDBC driver: i.e., persistence providers are
     * not expected to do such type conversion.
     *
     * @param dbData the data from the database column to be
     *               converted
     * @return the converted value to be stored in the entity
     * attribute
     */
    @Override
    public Map<String, Object> convertToEntityAttribute(String dbData) {
        try {
            if (!Strings.isNullOrEmpty(dbData)) {
                ObjectMapper mapper = JSONUtils.mapper();
                return mapper.readValue(dbData, new TypeReference<Map<String, Object>>() {
                    /**
                     * The only reason we define this method (and require implementation
                     * of <code>Comparable</code>) is to prevent constructing a
                     * reference without type information.
                     *
                     * @param o
                     */
                    @Override
                    public int compareTo(TypeReference<Map<String, Object>> o) {
                        return super.compareTo(o);
                    }
                });
            }
            return null;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
