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

package io.zyient.base.core.keystore.settings;

import io.zyient.base.common.config.Config;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JavaKeyStoreSettings extends KeyStoreSettings{
    public static final String CONFIG_KEYSTORE_FILE = "path";
    public static final String CONFIG_CIPHER_ALGO = "cipher.algo";
    public static final String CONFIG_KEYSTORE_TYPE = "type";
    private static final String KEYSTORE_TYPE = "PKCS12";

    @Config(name = CONFIG_KEYSTORE_FILE)
    private String keyStoreFile;
    @Config(name = CONFIG_CIPHER_ALGO, required = false)
    private String cipherAlgo = CIPHER_TYPE;
    @Config(name = CONFIG_KEYSTORE_TYPE, required = false)
    private String keyStoreType = KEYSTORE_TYPE;

}
