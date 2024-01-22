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

package io.zyient.core.filesystem.impl.azure;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.core.filesystem.impl.RemoteFileSystemSettings;
import lombok.Getter;
import lombok.Setter;


/**
 * <pre>
 *     <fs>
 *         <fileSystems>
 *             <fileSystem>
 *                 <type>[FS class]</type>
 *                 <name>[File System name, must be unique in a namespace]</name>
 *                 <tmp>
 *                     <path>[temporary storage path (optional)</path>
 *                     <clean>[true|false, default=true]</clean>
 *                     <ttl>[Clean files after, in milliseconds]</ttl>
 *                 </tmp>
 *                 <zk> -- Optional
 *                     <connection>[zookeeper connection name]</connection>
 *                     <path>[zookeeper path]</path>
 *                     <lockTimeout>[distributed lock timeout (optional)</lockTimeout>
 *                 </zk>
 *                 <hierarchical>[true|false, default = false]</hierarchical>
 *                 <client>
 *                     <endpointUrl>[End-point URL]</endpointUrl>
 *                     <authClass>[Authentication implementing class (io.zyient.cdc.core.io.impl.azure.SharedKeyAuth)]</authClass>
 *                     <account>[Account name]</account>
 *                     <auth>
 *                          <authKey>[Authentication Key]</authKey>
 *                     </auth>
 *                 </client>
 *                 <writer>
 *                     <flush>
 *                         <size>[Flush trigger size (in bytes), default = 32MB]</size>
 *                         <interval>[Flush interval (in milliseconds), default = 1min</interval>
 *                     </flush>
 *                     <threads>[Max #of upload threads, default = 4]</threads>
 *                 </writer>
 *                 <uploadTimeout>[File Upload timeout, default = 15 sec.]</uploadTimeout>
 *                 <compressed>[true|false, default=false]</compressed>
 *                 <containers>
 *                     <container>
 *                         <domain>[Domain name]</domain>
 *                         <container>[Azure Container name]</container>
 *                         <path>[Domain Root path]</path>
 *                     </container>
 *                     ...
 *                     <default>[Default domain name]</default>
 *                 </containers>
 *             </fileSystem>
 *             ...
 *         </fileSystems>
 *     </fs>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AzureFileSystemSettings extends RemoteFileSystemSettings {
    @Config(name = "hierarchical", required = false, type = Boolean.class)
    private boolean useHierarchical = false;
    @Config(name = "uploadTimeout", required = false, type = Long.class)
    private long uploadTimeout = 15; // 15 Seconds
    private AzureFsClientSettings clientSettings;

    public AzureFileSystemSettings() {
        setType(AzureFileSystem.class.getCanonicalName());
    }
}