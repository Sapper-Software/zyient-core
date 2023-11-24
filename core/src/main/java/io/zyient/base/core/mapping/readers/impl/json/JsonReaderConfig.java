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

package io.zyient.base.core.mapping.readers.impl.json;

import com.google.common.base.Preconditions;
import io.zyient.base.core.mapping.SourceTypes;
import io.zyient.base.core.mapping.model.InputContentInfo;
import io.zyient.base.core.mapping.readers.InputReader;
import io.zyient.base.core.mapping.readers.InputReaderConfig;
import io.zyient.base.core.mapping.readers.settings.JsonReaderSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class JsonReaderConfig extends InputReaderConfig {
    public JsonReaderConfig() {
        super(new SourceTypes[]{SourceTypes.JSON}, JsonReaderSettings.class);
    }

    @Override
    protected void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {

    }

    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        Preconditions.checkState(settings() instanceof JsonReaderSettings);
        return new JsonInputReader()
                .contentInfo(contentInfo)
                .settings(settings());
    }
}
