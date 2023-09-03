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

package ai.sapper.cdc.core.io.encryption;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;

public abstract class EncryptionHandler {
    protected final String key;
    protected final BaseEnv<?> env;

    public EncryptionHandler(@NonNull BaseEnv<?> env,
                             @NonNull String key) {
        this.env = env;
        this.key = key;
    }

    public abstract void encrypt(@NonNull File source,
                                 @NonNull FileInode inode,
                                 Context context,
                                 @NonNull File outfile) throws IOException;

    public abstract void decrypt(@NonNull File source,
                                 @NonNull FileInode inode,
                                 Context context,
                                 @NonNull File outfile) throws IOException;
}
