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

package io.zyient.core.persistence.impl.solr;

import lombok.NonNull;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

public class JSONUpdateRequest extends AbstractUpdateRequest {
    private final InputStream jsonInputStream;

    public JSONUpdateRequest(@NonNull InputStream jsonInputStream,
                             @NonNull String requestPath) {
        super(METHOD.POST, requestPath);
        this.jsonInputStream = jsonInputStream;
        this.setParam("json.command", "false");
        this.setSplit("/");
    }

    public void addFieldMapping(@NonNull String field,
                                @NonNull String jsonPath) {
        getParams().add("f", field + ":" + jsonPath);
    }

    public void setSplit(@NonNull String jsonPath) {
        setParam("split", jsonPath);
    }

    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
        ContentStream jsonContentStream = new InputStreamContentStream(
                jsonInputStream, "application/json");
        return Collections.singletonList(jsonContentStream);
    }

    private static class InputStreamContentStream extends ContentStreamBase {
        private final InputStream inputStream;

        public InputStreamContentStream(InputStream inputStream,
                                        String contentType) {
            this.inputStream = inputStream;
            this.setContentType(contentType);
        }

        @Override
        public InputStream getStream() throws IOException {
            return inputStream;
        }
    }
}