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
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.StringListParser;
import io.zyient.base.core.mapping.ReaderSettings;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.csv.DuplicateHeaderMode;
import org.apache.commons.csv.QuoteMode;

import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SeperatedReaderSettings extends ReaderSettings {
    @Config(name = "header.use", required = false, type = Boolean.class)
    private boolean hasHeader = false;
    @Config(name = "header.columns", required = false, parser = StringListParser.class)
    private List<String> headers;
    @Config(name = "override.delimiter", required = false)
    private String delimiter = null;
    @Config(name = "override.escapeChar", required = false)
    private String escape = null;
    @Config(name = "override.quoteChar", required = false)
    private String quote = null;
    @Config(name = "override.recordSeparator", required = false)
    private String recordSeparator = null;
    @Config(name = "override.quoteMode", required = false, type = QuoteMode.class)
    private QuoteMode quoteMode = null;
    @Config(name = "override.nullString", required = false)
    private String nullString = null;
    @Config(name = "override.ignoreEmptyLines", required = false)
    private Boolean ignoreEmptyLines = null;
    @Config(name = "override.duplicateHeaders", required = false, type = DuplicateHeaderMode.class)
    private DuplicateHeaderMode duplicateHeaders = null;
}
