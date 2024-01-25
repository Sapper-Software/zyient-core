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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.ws.WebServiceClient;
import io.zyient.base.core.utils.FileTypeDetector;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.core.sdk.model.content.Content;
import io.zyient.core.sdk.response.content.ContentCreateResponse;
import lombok.NonNull;

import java.io.File;
import java.util.List;

public class ContentCreateRequestBuilder {
    public static final String SERVICE_CONTENT_CREATE = "/create";
    public static final String SERVICE_FIELD_FILE = "document";
    public static final String SERVICE_FIELD_REQUEST = "entity";

    private final WebServiceClient client;
    private final ContentCreateRequest request = new ContentCreateRequest();
    private File path;

    public ContentCreateRequestBuilder(@NonNull WebServiceClient client) {
        this.client = client;
    }

    public ContentCreateRequestBuilder withContentName(@NonNull String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        request.setName(name);
        return this;
    }

    public ContentCreateRequestBuilder withMimeType(@NonNull String mimeType) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(mimeType));
        request.setMimeType(mimeType);
        return this;
    }

    public ContentCreateRequestBuilder withSourcePath(@NonNull String sourcePath) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sourcePath));
        request.setSourcePath(sourcePath);
        return this;
    }


    public ContentCreateRequestBuilder withFile(@NonNull File file) {
        Preconditions.checkArgument(file.exists());
        path = file;
        return this;
    }

    public ContentCreateResponse execute(List<String> params) throws Exception {
        ContentCreateResponse response = new ContentCreateResponse();
        response.setRequest(request);
        try {
            request.validate();
            if (path == null || !path.exists()) {
                throw new ValidationException("Path is not specified or file not found.");
            }
            if (Strings.isNullOrEmpty(request.getMimeType())) {
                FileTypeDetector detector = new FileTypeDetector(path);
                SourceTypes t = detector.detect();
                if (t != null && t != SourceTypes.UNKNOWN)
                    withMimeType(t.getMimeType());
            }
            Content content = client.upload(SERVICE_CONTENT_CREATE,
                    Content.class,
                    request,
                    params,
                    SERVICE_FIELD_FILE,
                    SERVICE_FIELD_REQUEST,
                    path);
            response.setContent(content);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            response.setError(ex);
        }
        return response;
    }
}
