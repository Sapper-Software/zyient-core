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

package io.zyient.core.mapping.transformers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.Strings;
import io.zyient.core.mapping.DataException;
import io.zyient.core.mapping.mapper.MappingSettings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class DeSerializer<T> extends StdDeserializer<T> {
    public static final String __CONFIG_PATH = "deSerializer";

    protected String name;
    private final Class<T> type;

    protected DeSerializer(Class<T> t) {
        super(t);
        type = t;
    }

    public abstract DeSerializer<T> configure(@NonNull MappingSettings settings) throws ConfigurationException;

    public abstract DeSerializer<T> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    public abstract T transform(@NonNull Object value) throws DataException;

    public abstract String serialize(@NonNull T value) throws DataException;

    /**
     * Method that can be called to ask implementation to deserialize
     * JSON content into the value type this serializer handles.
     * Returned instance is to be constructed by method itself.
     * <p>
     * Pre-condition for this method is that the parser points to the
     * first event that is part of value to deserializer (and which
     * is never JSON 'null' literal, more on this below): for simple
     * types it may be the only value; and for structured types the
     * Object start marker.
     * Post-condition is that the parser will point to the last
     * event that is part of deserialized value (or in case deserialization
     * fails, event that was not recognized or usable, which may be
     * the same event as the one it pointed to upon call).
     * <p>
     * Note that this method is never called for JSON null literal,
     * and thus deserializers need (and should) not check for it.
     *
     * @param jp   Parsed used for reading JSON content
     * @param ctxt Context that can be used to access information about
     *             this deserialization activity.
     * @return Deserializer value
     */
    @Override
    public T deserialize(JsonParser jp,
                         DeserializationContext ctxt) throws IOException {
        String value = jp.getText();
        if (!Strings.isNullOrEmpty(value)) {
            try {
                return transform(value);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
        return null;
    }
}
