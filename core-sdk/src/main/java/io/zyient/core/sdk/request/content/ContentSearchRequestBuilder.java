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
import io.zyient.core.sdk.response.content.ContentSearchResponse;
import lombok.NonNull;

import java.util.List;

public class ContentSearchRequestBuilder {
    public static final String SERVICE_CONTENT_SEARCH = "/search";

    private final WebServiceClient client;

    public ContentSearchRequestBuilder withPageNo(int pageNo) {
        request.setPageNo(pageNo);
        return this;
    }

    public ContentSearchRequestBuilder withPageSize(int pageSize) {
        request.setPageSize(pageSize);
        return this;
    }

    private final ContentSearchRequest request = new ContentSearchRequest();

    public ContentSearchRequestBuilder(@NonNull WebServiceClient client) {
        this.client = client;
    }

    @SuppressWarnings("unchecked")
    public ContentSearchResponse execute(List<String> params) throws Exception {
        ContentSearchResponse response = new ContentSearchResponse();
        response.setRequest(request);
        try {
            request.validate();
            List<Content> contents = client.post(SERVICE_CONTENT_SEARCH,
                    List.class,
                    request,
                    params,
                    FileUtils.MIME_TYPE_JSON);
            if (contents != null) {
                response.setRecords(contents);
                response.setCount(contents.size());
            } else {
                response.setCount(0);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            response.setError(ex);
        }
        return response;
    }
}
