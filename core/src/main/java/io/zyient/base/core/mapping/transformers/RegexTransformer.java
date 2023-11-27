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

package io.zyient.base.core.mapping.transformers;

import com.google.common.base.Strings;
import io.zyient.base.core.mapping.DataException;
import io.zyient.base.core.mapping.mapper.MappingSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@Accessors(fluent = true)
public class RegexTransformer extends DeSerializer<String> {
    private String regex;
    private String replace;
    private List<Integer> groups;
    private String format;
    @Setter(AccessLevel.NONE)
    private Pattern pattern;

    public RegexTransformer() {
        super(String.class);
    }

    @Override
    public DeSerializer<String> configure(@NonNull MappingSettings settings) throws ConfigurationException {
        if (Strings.isNullOrEmpty(regex)) {
            throw new ConfigurationException("Regex not specified...");
        }
        name = regex;
        pattern = Pattern.compile(regex);
        return this;
    }

    @Override
    public String transform(@NonNull Object source) throws DataException {
        if (source instanceof String value) {
            if (!Strings.isNullOrEmpty(replace)) {
                return ((String) source).replaceAll(regex, replace);
            }
            Matcher m = pattern.matcher(value);
            if (m.matches()) {
                if (Strings.isNullOrEmpty(format)) {
                    return value;
                }
                if (groups == null || groups.isEmpty()) {
                    throw new DataException("No regex groups specified...");
                }
                for (int group : groups) {
                    if (group > m.groupCount()) break;
                    String v = m.group(group);
                    String k = "{" + group + "}";
                    value = value.replace(k, v);
                }
                return value;
            }
            return null;
        }
        throw new DataException(String.format("Cannot transform to String. [source=%s]", source.getClass()));
    }
}
