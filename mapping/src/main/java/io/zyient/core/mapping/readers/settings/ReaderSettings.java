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
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.utils.SourceTypes;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ReaderSettings extends Settings {
    @Config(name = "name")
    private String name;
    @Config(name = "assumeFileType", required = false, type = SourceTypes.class)
    private SourceTypes assumeType;
    @Config(name = "readBatchSize", required = false, type = Integer.class)
    private int readBatchSize = 512;
    @Config(name = "passwordProtected", required = false, type = Boolean.class)
    private boolean passwordProtected;
    @Config(name = "encrypted", required = false, type = Boolean.class)
    private boolean encrypted;
    @Config(name = "decryptionKeyName", required = false)
    private String decryptionKeyName;
    @Config(name = "decryptionSecretName", required = false)
    private String decryptionSecretName;
}
