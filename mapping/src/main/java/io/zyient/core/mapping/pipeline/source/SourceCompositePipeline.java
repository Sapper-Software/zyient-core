/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.mapping.pipeline.source;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.RecordResponse;
import io.zyient.core.mapping.model.SourceMap;
import io.zyient.core.mapping.pipeline.CompositePipeline;
import io.zyient.core.mapping.pipeline.PipelineSource;
import io.zyient.core.mapping.pipeline.settings.CompositePipelineSettings;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.ReadResponse;
import lombok.NonNull;

public abstract class SourceCompositePipeline extends CompositePipeline implements PipelineSource {

    @Override
    public ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception {
        checkState();
        CompositePipelineSettings settings = (CompositePipelineSettings) settings();
        Preconditions.checkNotNull(settings);
        DefaultLogger.info(String.format("Running pipeline for entity. [name=%s]", name()));
        ReadResponse response = new ReadResponse();
        ReadCursor cursor = reader.open();
        while (true) {
            RecordResponse r = new RecordResponse();
            try {
                SourceMap data = cursor.next();
                if (data == null) break;
                r.setSource(data);
                response.incrementCount();
                r = process(data, context);
                response.add(r);
            } catch (ValidationException | ValidationExceptions ex) {
                String mesg = String.format("[file=%s][record=%d] Validation Failed: %s",
                        reader.input().getAbsolutePath(), response.getRecordCount(), ex.getLocalizedMessage());
                if (settings().isTerminateOnValidationError()) {
                    DefaultLogger.stacktrace(ex);
                    throw ex;
                } else {
                    DefaultLogger.warn(mesg);
                    r = errorResponse(r, null, ex);
                    response.add(r);
                }
            } catch (Exception e) {
                DefaultLogger.stacktrace(e);
                DefaultLogger.error(e.getLocalizedMessage());
                throw e;
            }
        }
        DefaultLogger.info(String.format("Processed [%d] records for entity. [name=%s]",
                response.getRecordCount(), name()));
        return response;
    }
}
