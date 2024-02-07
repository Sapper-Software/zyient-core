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

package io.zyient.core.mapping.readers.impl.separated;

import com.google.common.base.Preconditions;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.core.mapping.model.Column;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.InputReaderConfig;
import io.zyient.core.mapping.readers.settings.ReaderSettings;
import io.zyient.core.mapping.readers.settings.SeparatedReaderSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.Map;

public class SeparatedReaderConfig extends InputReaderConfig {
    private final static SourceTypes[] DEFAULT_SOURCE_TYPES = new SourceTypes[]{SourceTypes.CSV,
            SourceTypes.EXCEL_CSV,
            SourceTypes.TSV,
            SourceTypes.PSV,
            SourceTypes.RFC4180};

    public SeparatedReaderConfig() {
        super(DEFAULT_SOURCE_TYPES, SeparatedReaderSettings.class);
    }

    public SeparatedReaderConfig(Class<? extends ReaderSettings> cls) {
        super(DEFAULT_SOURCE_TYPES, cls);
    }

    @Override
    protected void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        Preconditions.checkState(settings() instanceof SeparatedReaderSettings);
        Map<Integer, Column> columns = Column.read(xmlConfig,
                SeparatedReaderSettings.__CONFIG_PATH_COLUMNS,
                Column.class);
        if (columns != null) {
            ((SeparatedReaderSettings) settings()).setHeaders(columns);
        }
    }

    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        Preconditions.checkState(settings() instanceof SeparatedReaderSettings);
        return new SeparatedInputReader()
                .contentInfo(contentInfo)
                .settings(settings());
    }
}
