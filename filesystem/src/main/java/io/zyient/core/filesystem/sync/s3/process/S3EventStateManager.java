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

package io.zyient.core.filesystem.sync.s3.process;

import io.zyient.core.filesystem.sync.SyncEventStateManager;
import io.zyient.core.filesystem.sync.s3.model.S3EventOffset;
import io.zyient.core.filesystem.sync.s3.model.S3EventProcessingState;

public class S3EventStateManager extends SyncEventStateManager<S3EventOffset> {
    public S3EventStateManager() {
        super(S3EventProcessingState.class);
    }
}
