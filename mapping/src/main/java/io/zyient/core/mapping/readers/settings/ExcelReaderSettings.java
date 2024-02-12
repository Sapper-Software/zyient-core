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
import io.zyient.core.mapping.readers.impl.excel.CellMissing;
import io.zyient.core.mapping.readers.impl.excel.ExcelHeader;
import io.zyient.core.mapping.readers.impl.excel.ExcelSheet;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ExcelReaderSettings extends ReaderSettings {
    private Map<Integer, Column> headers;
    private List<ExcelSheet> sheets;
    @Config(name = "columnPrefix", required = false)
    private String columnPrefix = "COLUMN_";
    @Config(name = "header.type", required = false, type = ExcelHeader.class)
    private ExcelHeader header = ExcelHeader.None;
    @Config(name = "header.skip", required = false, type = Boolean.class)
    private boolean skipHeader = true;
    @Config(name = "cell.missing", required = false, type = CellMissing.class)
    private CellMissing cellMissing = CellMissing.Both;

}
