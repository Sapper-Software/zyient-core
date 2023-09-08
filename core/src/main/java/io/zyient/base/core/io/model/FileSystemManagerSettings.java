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

package io.zyient.base.core.io.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <fs>
 *         <zkPath>[zookeeper path]</zkPath>
 *         <zkConnection>[zookeeper connection name]</zkConnection>
 *         <autoSave>[true|false, default=true]</autoSave>
 *     </fs>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileSystemManagerSettings extends Settings {
    @Config(name = "zkPath")
    private String zkBasePath;
    @Config(name = "zkConnection")
    private String zkConnection;
    @Config(name = "autoSave", required = false, type = Boolean.class)
    private boolean autoSave = true;
    @Config(name = "overwrite", required = false, type = Boolean.class)
    private boolean overwriteSettings = false;
}