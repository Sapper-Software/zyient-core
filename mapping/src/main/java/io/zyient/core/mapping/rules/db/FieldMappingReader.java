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

package io.zyient.core.mapping.rules.db;

import io.zyient.base.common.config.maps.StringMapValueParser;

public class FieldMappingReader extends StringMapValueParser {
    public static final String __CONFIG_PATH = "fieldMappings";
    public static final String CONFIG_NODE = "mapping";
    public static final String CONFIG_SOURCE = "source";
    public static final String CONFIG_TARGET = "target";

    public FieldMappingReader() {
        super(__CONFIG_PATH, CONFIG_NODE, CONFIG_SOURCE, CONFIG_TARGET);
    }
}
