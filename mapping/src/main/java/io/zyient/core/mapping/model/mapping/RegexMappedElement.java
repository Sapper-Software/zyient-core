/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.model.mapping;


import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.lists.StringListParser;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class RegexMappedElement extends MappedElement {
    @Config(name = "name")
    private String name;
    @Config(name = "regex", required = true)
    private String regex;
    @Config(name = "replaceWith", required = false)
    private String replace;
    @Config(name = "groups", required = false, parser = StringListParser.class)
    private List<String> groups;
    @Config(name = "format", required = false)
    private String format;

    public RegexMappedElement() {
    }

    public RegexMappedElement(@NonNull MappedElement source) {
        super(source);
        if (source instanceof RegexMappedElement) {
            name = ((RegexMappedElement) source).name;
            regex = ((RegexMappedElement) source).regex;
            replace = ((RegexMappedElement) source).replace;
            groups = ((RegexMappedElement) source).groups;
            format = ((RegexMappedElement) source).format;
        }
    }
}
