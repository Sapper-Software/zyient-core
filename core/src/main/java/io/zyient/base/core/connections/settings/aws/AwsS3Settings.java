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

package io.zyient.base.core.connections.settings.aws;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.core.connections.aws.auth.S3StorageAuth;
import io.zyient.base.core.connections.aws.auth.S3StorageAuthSettings;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AwsS3Settings extends ConnectionSettings {
    public static final String __CONFIG_PATH = "s3";
    public static final String CONFIG_REGION = "region";
    public static final String CONFIG_URL = "endpoint";

    @Config(name = CONFIG_URL, required = false)
    private String endpoint;
    @Config(name = CONFIG_REGION)
    private String region;
    private Class<? extends S3StorageAuth> authHandler;
    private S3StorageAuthSettings authSettings;

    @Override
    public void validate() throws Exception {
        super.validate();
        if (authHandler != null) {
            if (authSettings == null) {
                throw new Exception("Auth handler settings not specified...");
            }
        }
    }
}
