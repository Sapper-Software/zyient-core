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

package io.zyient.base.core.mapping.readers.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;

import java.text.SimpleDateFormat;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class XmlReaderSettings extends ReaderSettings {
    @Config(name = "basePath")
    private String basePath;
    @Config(name = "isArray", required = false, type = Boolean.class)
    private boolean isArray = false;
    @Config(name = "dateFormat", required = false)
    private String dateFormat = null;

    public XmlMapper getXmlMapper() {
        XmlMapper mapper = new XmlMapper();
        if (!Strings.isNullOrEmpty(dateFormat)) {
            SimpleDateFormat fmt = new SimpleDateFormat(dateFormat);
            mapper.setDateFormat(fmt);
        }
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
                .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
                .configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);

        return mapper;
    }
}
