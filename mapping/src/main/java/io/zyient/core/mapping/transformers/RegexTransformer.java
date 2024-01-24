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

import com.google.common.base.Strings;
import io.zyient.core.mapping.mapper.MappingSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.SerializationException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@Accessors(fluent = true)
public class RegexTransformer implements Transformer<String> {
    private String regex;
    private String replace;
    private Set<String> groups;
    private String format;
    @Setter(AccessLevel.NONE)
    private Pattern pattern;
    private String name;

    public RegexTransformer withGroups(@NonNull Collection<String> groups) {
        this.groups = new HashSet<>(groups);
        return this;
    }

    @Override
    public Transformer<String> configure(@NonNull MappingSettings settings) throws ConfigurationException {
        if (Strings.isNullOrEmpty(regex)) {
            throw new ConfigurationException("Regex not specified...");
        }
        name = regex;
        pattern = Pattern.compile(regex);
        return this;
    }

    @Override
    public String read(@NonNull Object source) throws SerializationException {
        if (source instanceof String value) {
            if (!Strings.isNullOrEmpty(replace)) {
                return ((String) source).replaceAll(regex, replace);
            }
            Matcher m = pattern.matcher(value);
            if (Strings.isNullOrEmpty(format)) {
                return value;
            }
            if (groups != null && !groups.isEmpty()) {
                value = format;
                int matchCount = 1;
                while (m.find()) {
                    for (int ii = 1; ii <= m.groupCount(); ii++) {
                        String k = String.format("%d:%d", matchCount, ii);
                        if (groups.contains(k)) {
                            String v = m.group(ii);
                            String r = String.format("{%s}", k);
                            value = value.replace(r, v);
                        }
                    }
                    matchCount++;
                }
                return value;
            }
            return null;
        }
        throw new SerializationException(String.format("Cannot transform to String. [source=%s]", source.getClass()));
    }
}