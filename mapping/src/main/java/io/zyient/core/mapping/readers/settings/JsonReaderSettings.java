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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.zyient.base.common.GlobalConstants;
import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;

import java.text.SimpleDateFormat;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class JsonReaderSettings extends ReaderSettings {
    @Config(name = "basePath", required = false)
    private String basePath;
    @Config(name = "isArray", required = false, type = Boolean.class)
    private boolean array = true;
    @Config(name = "dateFormat", required = false)
    private String dateFormat = null;

    public ObjectMapper getObjectMapper() {
        ObjectMapper mapper = GlobalConstants.getJsonMapper();
        if (!Strings.isNullOrEmpty(dateFormat)) {
            SimpleDateFormat fmt = new SimpleDateFormat(dateFormat);
            mapper.setDateFormat(fmt);
        }
        return mapper;
    }
}
