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

package io.zyient.base.core.mapping.readers.impl.positional;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.core.mapping.SourceTypes;
import io.zyient.base.core.mapping.model.Column;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.mapping.readers.InputReaderConfig;
import io.zyient.base.core.mapping.readers.settings.PositionalReaderSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PositionalReaderConfig extends InputReaderConfig {
    public PositionalReaderConfig() {
        super(new SourceTypes[]{SourceTypes.POSITIONAL}, PositionalReaderSettings.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        Preconditions.checkState(settings() instanceof PositionalReaderSettings);
        Map<Integer, Column> columns = new HashMap<>();
        HierarchicalConfiguration<ImmutableNode> config = xmlConfig.configurationAt(__CONFIG_PATH_COLUMNS);
        List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(__CONFIG_PATH_COLUMN);
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            Class<? extends Column> type = (Class<? extends Column>) ConfigReader.readType(node);
            if (type == null) {
                type = Column.class;
            }
            Column column = ConfigReader.read(node, type);
            column.validate();
            columns.put(column.getIndex(), column);
        }
        for (int ii = 0; ii < columns.size(); ii++) {
            if (!columns.containsKey(ii)) {
                throw new Exception(String.format("Missing column index. [index=%d]", ii));
            }
        }
        ((PositionalReaderSettings) settings()).setColumns(columns);
    }

    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        return new PositionalInputReader()
                .contentInfo(contentInfo)
                .settings(settings());
    }
}
