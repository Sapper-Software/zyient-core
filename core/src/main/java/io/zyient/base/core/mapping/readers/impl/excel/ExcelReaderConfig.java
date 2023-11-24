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

package io.zyient.base.core.mapping.readers.impl.excel;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.mapping.SourceTypes;
import io.zyient.base.core.mapping.model.Column;
import io.zyient.base.core.mapping.model.ExcelColumn;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.mapping.readers.InputReaderConfig;
import io.zyient.base.core.mapping.readers.settings.ExcelReaderSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExcelReaderConfig extends InputReaderConfig {
    public static final String __CONFIG_PATH_SHEETS = "sheets";
    public static final String __CONFIG_PATH_SHEET = "sheet";

    public ExcelReaderConfig() {
        super(new SourceTypes[]{SourceTypes.EXCEL}, ExcelReaderSettings.class);
    }

    @Override
    protected void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        Preconditions.checkState(settings() instanceof ExcelReaderSettings);
        Map<Integer, Column> columns = Column.read(xmlConfig, ExcelColumn.class);
        if (columns != null) {
            ((ExcelReaderSettings) settings()).setHeaders(columns);
        }
        HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH_SHEETS);
        List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(__CONFIG_PATH_SHEET);
        List<ExcelSheet> sheets = new ArrayList<>(nodes.size());
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            ExcelSheet sheet = ConfigReader.read(node, ExcelSheet.class);
            sheet.validate();
            sheets.add(sheet);
        }
        ((ExcelReaderSettings) settings()).setSheets(sheets);
    }

    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        return null;
    }
}
