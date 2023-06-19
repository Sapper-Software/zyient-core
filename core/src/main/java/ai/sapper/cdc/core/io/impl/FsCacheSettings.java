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

package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <cache>
 *         <size>[Max Cache record count, default = 128]</size>
 *         <timeout>[Cache entry timeout, default = 60sec]</timeout>
 *         <download>
 *             <timeout>[File download timeout, default = 60sec]</timeout>
 *         </download>
 *     </cache>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FsCacheSettings extends Settings {
    public static final String __CONFIG_PATH = "cache";
    public static final String CONFIG_DOWNLOAD_TIMEOUT = "download.timeout";

    private static final int CACHE_SIZE = 128;
    private static final long CACHE_TIMEOUT = 5 * 60 * 1000;
    public static final long READER_DOWNLOAD_TIMEOUT = 60 * 1000;

    @Config(name = "size", required = false, type = Integer.class)
    private int cacheSize = CACHE_SIZE;
    @Config(name = "timeout", required = false, type = Long.class)
    private long cacheTimeout = CACHE_TIMEOUT;
    @Config(name = CONFIG_DOWNLOAD_TIMEOUT, required = false, type = Long.class)
    private long downloadTimeout = READER_DOWNLOAD_TIMEOUT;
}