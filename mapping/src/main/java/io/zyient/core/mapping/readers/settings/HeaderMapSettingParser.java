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

package io.zyient.core.mapping.readers.settings;

import io.zyient.base.common.config.maps.BooleanMapValueParser;

public class HeaderMapSettingParser extends BooleanMapValueParser {
    public static final String __SECTION = "section.header";
    public static final String CONFIG_FIELDS = "fields";
    public static final String CONFIG_FIELD = "field";
    public static final String CONFIG_REQUIRED = "required";

    public HeaderMapSettingParser() {
        super(__SECTION, CONFIG_FIELDS, CONFIG_FIELD, CONFIG_REQUIRED);
    }
}
