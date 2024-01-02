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

package io.zyient.core.sdk.request.content;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.ws.WebServiceClient;
import io.zyient.base.core.utils.FileUtils;
import io.zyient.core.sdk.model.DownloadResponse;
import io.zyient.core.sdk.model.content.ContentId;
import io.zyient.core.sdk.response.content.ContentGetResponse;
import lombok.NonNull;

import java.util.List;

public class ContentGetRequestBuilder {
    public static final String SERVICE_CONTENT_GET = "/get";

    private final WebServiceClient client;
    private final ContentGetRequest request = new ContentGetRequest();

    public ContentGetRequestBuilder(@NonNull WebServiceClient client) {
        this.client = client;
    }

    public ContentGetRequestBuilder withContentId(ContentId contentId) {
        request.setContentId(contentId);
        return this;
    }

    public ContentGetResponse execute(List<String> params) throws Exception {
        ContentGetResponse response = new ContentGetResponse();
        response.setRequest(request);
        try {
            request.validate();
            DownloadResponse r = client.post(SERVICE_CONTENT_GET,
                    DownloadResponse.class,
                    request,
                    params,
                    FileUtils.MIME_TYPE_JSON);
            response.setResponse(r);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            response.setError(ex);
        }
        return response;
    }
}
