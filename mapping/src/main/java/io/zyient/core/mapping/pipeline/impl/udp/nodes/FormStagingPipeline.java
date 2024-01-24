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

package io.zyient.core.mapping.pipeline.impl.udp.nodes;

import io.zyient.base.common.model.Context;
import io.zyient.core.mapping.model.RecordResponse;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.pipeline.impl.udp.model.FormContext;
import io.zyient.core.mapping.pipeline.staging.StagingPipeline;
import lombok.NonNull;

public class FormStagingPipeline extends StagingPipeline<FormContext> {

    @Override
    public RecordResponse execute(@NonNull SourceMap data, Context context) throws Exception {
        return super.execute(data, context);
    }
}
