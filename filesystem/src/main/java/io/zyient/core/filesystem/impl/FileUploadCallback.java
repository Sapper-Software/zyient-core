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

package io.zyient.core.filesystem.impl;

import io.zyient.core.filesystem.model.FileInode;
import lombok.NonNull;

public interface FileUploadCallback {
    void onSuccess(@NonNull FileInode inode, @NonNull Object response, boolean clearLock);

    void onError(@NonNull FileInode inode, @NonNull Throwable error);
}
