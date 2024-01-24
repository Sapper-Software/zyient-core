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

package io.zyient.core.sdk.request.content;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.ws.WebServiceClient;
import io.zyient.base.core.utils.FileUtils;
import io.zyient.core.sdk.model.content.Content;
import io.zyient.core.sdk.model.content.ContentId;
import io.zyient.core.sdk.response.content.ContentUpdateResponse;
import lombok.NonNull;

import java.util.List;

public class ContentUpdateRequestBuilder<E extends Enum<?>> {
    public static final String SERVICE_CONTENT_CREATE = "/update/state";

    private final WebServiceClient client;
    private final ContentUpdateRequest request = new ContentUpdateRequest();

    public ContentUpdateRequestBuilder(@NonNull WebServiceClient client) {
        this.client = client;
    }

    public ContentUpdateRequestBuilder<E> withContentId(@NonNull ContentId contentId) {
        request.setContentId(contentId);
        return this;
    }

    public ContentUpdateRequestBuilder<E> withContentState(@NonNull E state) {
        request.setContentState(state.name());
        return this;
    }


    public ContentUpdateResponse execute(List<String> params) throws Exception {
        ContentUpdateResponse response = new ContentUpdateResponse();
        response.setRequest(request);
        try {
            request.validate();
            Content content = client.post(SERVICE_CONTENT_CREATE,
                    Content.class,
                    request,
                    params,
                    FileUtils.MIME_TYPE_JSON);
            response.setContent(content);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            response.setError(ex);
        }
        return response;
    }
}
