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

package io.zyient.base.core.messaging;

import io.zyient.base.common.config.ConfigReader;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class MessagingProcessorConfig extends ConfigReader {
    public MessagingProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull String path,
                                    @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(config, path, settingsType);
    }

    public MessagingProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                    @NonNull Class<? extends MessagingProcessorSettings> settingsType) {
        super(config, settingsType);
    }

    @Override
    public void read() throws ConfigurationException {
        super.read();
        MessagingProcessorSettings settings = (MessagingProcessorSettings) settings();
        if (settings.getErrorsBuilderType() != null) {
            if (settings.getErrorsBuilderSettingsType() == null) {
                throw new ConfigurationException("Error Queue builder specified, but builder settings not specified.");
            }
        }
    }
}
