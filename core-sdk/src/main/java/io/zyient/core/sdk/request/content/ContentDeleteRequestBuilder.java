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
import io.zyient.base.core.model.UserOrRole;
import io.zyient.base.core.utils.FileUtils;
import io.zyient.core.sdk.model.DeleteResponse;
import io.zyient.core.sdk.model.content.ContentId;
import io.zyient.core.sdk.response.content.ContentDeleteResponse;
import lombok.NonNull;

import java.util.List;

public class ContentDeleteRequestBuilder {
    public static final String SERVICE_CONTENT_DELETE = "/delete";

    private final WebServiceClient client;
    private final ContentDeleteRequest request = new ContentDeleteRequest();

    public ContentDeleteRequestBuilder(@NonNull WebServiceClient client) {
        this.client = client;
    }

    public ContentDeleteRequestBuilder withRequester(UserOrRole user) {
        request.setUser(user);
        return this;
    }

    public ContentDeleteRequestBuilder withContentId(ContentId contentId) {
        request.setContentId(contentId);
        return this;
    }

    public ContentDeleteResponse execute(List<String> params) throws Exception {
        ContentDeleteResponse response = new ContentDeleteResponse();
        response.setRequest(request);
        try {
            request.validate();
            DeleteResponse r = client.post(SERVICE_CONTENT_DELETE,
                    DeleteResponse.class,
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
