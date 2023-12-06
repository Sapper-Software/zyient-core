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

package io.zyient.base.core.io.impl;

import io.zyient.base.core.io.model.Inode;
import lombok.NonNull;

public interface PostOperationVisitor {
    void visit(@NonNull Operation op,
               @NonNull OperationState state,
               Inode inode,
               Throwable error);

    enum OperationState {
        Completed, Error
    }

    enum Operation {
        Create, Update, Delete, Read, Upload, Download;
    }
}
