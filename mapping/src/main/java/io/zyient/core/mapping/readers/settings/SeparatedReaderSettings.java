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
import io.zyient.base.common.config.Config;
import io.zyient.core.mapping.model.Column;
import io.zyient.core.mapping.readers.impl.separated.SeparatedReaderTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.DuplicateHeaderMode;
import org.apache.commons.csv.QuoteMode;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SeparatedReaderSettings extends ReaderSettings {
    public static final String __CONFIG_PATH_COLUMNS = "header.columns";

    @Config(name = "type", required = false, type = SeparatedReaderTypes.class)
    private SeparatedReaderTypes type = SeparatedReaderTypes.DEFAULT;
    @Config(name = "header.present", required = false, type = Boolean.class)
    private Boolean hasHeader = null;
    private Map<Integer, Column> headers = null;
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
    @Config(name = "columnPrefix", required = false)
    private String columnPrefix = "COLUMN_";

    public CSVFormat setup(@NonNull CSVFormat format) throws Exception {
        CSVFormat.Builder builder = format.builder();
        if (checkNotNull(hasHeader) && hasHeader) {
            if (headers == null || headers.isEmpty()) {
                builder.setHeader();
            } else {
                String[] array = new String[headers.size()];
                for (int ii = 0; ii < headers.size(); ii++) {
                    Column column = headers.get(ii);
                    if (column == null) {
                        throw new Exception(String.format("Missing column. [sequence=%s]", ii));
                    }
                    array[ii] = column.getName();
                }
                builder.setHeader(array);
            }
            builder.setSkipHeaderRecord(true);
        } else {
            builder.setSkipHeaderRecord(false);
        }
        if (checkNotNull(delimiter)) {
            builder.setDelimiter(delimiter);
        }
        if (checkNotNull(escape)) {
            builder.setEscape(escape.trim().charAt(0));
        }
        if (checkNotNull(quote)) {
            builder.setQuote(quote.trim().charAt(0));
        }
        if (checkNotNull(recordSeparator)) {
            builder.setRecordSeparator(recordSeparator);
        }
        if (checkNotNull(quoteMode)) {
            builder.setQuoteMode(quoteMode);
        }
        if (checkNotNull(nullString)) {
            builder.setNullString(nullString);
        }
        if (checkNotNull(ignoreEmptyLines)) {
            builder.setIgnoreEmptyLines(ignoreEmptyLines);
        }
        if (checkNotNull(duplicateHeaders)) {
            builder.setDuplicateHeaderMode(duplicateHeaders);
        }
        return builder.build();
    }

    private boolean checkNotNull(Object obj) {
        return (obj != null);
    }
}
