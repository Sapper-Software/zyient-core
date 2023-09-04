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

package io.zyient.base.core.io.impl.s3;

import io.zyient.base.common.config.Config;
import io.zyient.base.core.io.impl.RemoteFileSystemSettings;
import lombok.Getter;
import lombok.Setter;


/**
 * <pre>
 *     <fs>
 *         <fileSystems>
 *             <fileSystem>
 *                 <type>[FS class]</type>
 *                 <name>[File System name, must be unique in a namespace]</name>
 *                 <region>[AWS S3 region]</region>
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
 *                 <writer>
 *                     <flush>
 *                         <size>[Flush trigger size (in bytes), default = 32MB]</size>
 *                         <interval>[Flush interval (in milliseconds), default = 1min</interval>
 *                     </flush>
 *                     <threads>[Max #of upload threads, default = 4]</threads>
 *                 </writer>
 *                 <compressed>[true|false, default=false]</compressed>
 *                 <containers>
 *                     <container>
 *  *                         <domain>[Domain name]</domain>
 *  *                         <bucket>[S3 Bucket name]</bucket>
 *  *                         <path>[Domain Root path]</path>
 *  *                  </container>
 *                     ...
 *                     <default>[Default domain]</default>
 *                 </containers>
 *             </fileSystem>
 *             ...
 *         </fileSystems>
 *     </fs>
 * </pre>
 */
@Getter
@Setter
public  class S3FileSystemSettings extends RemoteFileSystemSettings {
    public static final String CONFIG_REGION = "region";

    @Config(name = CONFIG_REGION)
    private String region;

    public S3FileSystemSettings() {
        setType(S3FileSystem.class.getCanonicalName());
    }
}
