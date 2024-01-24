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

package io.zyient.core.mapping.mapper.db;

import io.zyient.base.common.config.lists.StringListParser;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.core.mapping.model.mapping.MappedElement;
import io.zyient.core.mapping.model.mapping.RegexMappedElement;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Entity
@Table(name = "m_regex_defs")
public class DBRegexMappingDef extends DBMappingDef {
    @Column(name = "name")
    private String name;
    @Column(name = "regex")
    private String regex;
    @Column(name = "replace_with")
    private String replace;
    @Column(name = "regex_groups")
    private String groups;
    @Column(name = "replace_with_format")
    private String format;


    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void validate() throws ValidationExceptions {
        super.validate();
        ValidationExceptions errors = ValidationExceptions.checkValue(name,
                "Name is null/empty", null);
        errors = ValidationExceptions.checkValue(regex,
                "No regular expression specified...", errors);
        if (errors != null)
            throw errors;
    }

    @Override
    public MappedElement as() throws Exception {
        MappedElement me = super.as();
        RegexMappedElement rme = new RegexMappedElement(me);
        rme.setName(name);
        rme.setRegex(regex);
        rme.setReplace(replace);
        StringListParser parser = new StringListParser();
        List<String> values = parser.parse(groups);
        rme.setGroups(values);
        rme.setFormat(format);
        return rme;
    }
}
