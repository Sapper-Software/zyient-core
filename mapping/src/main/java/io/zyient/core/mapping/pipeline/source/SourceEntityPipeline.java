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

package io.zyient.core.mapping.pipeline.source;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.RecordResponse;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.pipeline.EntityPipeline;
import io.zyient.core.mapping.pipeline.PipelineSource;
import io.zyient.core.mapping.pipeline.settings.PersistedEntityPipelineSettings;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.ReadResponse;
import io.zyient.core.persistence.TransactionDataStore;
import io.zyient.core.persistence.impl.rdbms.RdbmsDataStore;
import lombok.NonNull;

public class SourceEntityPipeline<K extends IKey, E extends IEntity<K>> extends EntityPipeline<K, E>
        implements PipelineSource {

    @Override
    @SuppressWarnings(("unchecked"))
    public ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception {
        checkState();
        PersistedEntityPipelineSettings settings = (PersistedEntityPipelineSettings) settings();
        Preconditions.checkNotNull(settings);
        DefaultLogger.info(String.format("Running pipeline for entity. [type=%s]", entityType().getCanonicalName()));
        ReadResponse response = new ReadResponse();
        beingTransaction();
        try {
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
                    response.incrementCommitCount();

                } catch (ValidationException | ValidationExceptions ex) {
                    String mesg = String.format("[file=%s][record=%d] Validation Failed: %s",
                            reader.input().getAbsolutePath(), response.getRecordCount(), ex.getLocalizedMessage());
                    ValidationExceptions ve = ValidationExceptions.add(new ValidationException(mesg), null);
                    if (settings().isTerminateOnValidationError()) {
                        DefaultLogger.stacktrace(ex);
                        throw ve;
                    } else {
                        response.incrementCount();
                        DefaultLogger.warn(mesg);
                        r = errorResponse(r, null, ex);
                        response.add(r);
                    }
                } catch (Exception e) {
                    rollback();
                    DefaultLogger.stacktrace(e);
                    DefaultLogger.error(e.getLocalizedMessage());
                    throw e;
                }
            }
            commit();
        } catch (Exception e) {
            rollback();
            throw e;
        }


        DefaultLogger.info(String.format("Processed [%d] records for entity. [type=%s]",
                response.getRecordCount(), entityType().getCanonicalName()));
        return response;
    }
}
