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

package io.zyient.core.sdk.request.fs;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.connections.ws.WebServiceClient;
import io.zyient.core.sdk.model.fs.FileHandle;
import io.zyient.core.sdk.model.fs.FileType;
import io.zyient.core.sdk.response.fs.FileCreateResponse;
import lombok.NonNull;

import java.io.File;
import java.util.List;
import java.util.Map;

public class FileCreateRequestBuilder {
    public static final String SERVICE_CONTENT_CREATE = "/create";
    public static final String SERVICE_FIELD_FILE = "document";
    public static final String SERVICE_FIELD_REQUEST = "entity";

    private final WebServiceClient client;
    private final FileCreateRequest request = new FileCreateRequest();
    private File path;

    public FileCreateRequestBuilder(@NonNull WebServiceClient client) {
        this.client = client;
    }

    public FileCreateRequestBuilder withDomain(@NonNull String domain) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
        request.setDomain(domain);
        return this;
    }

    public FileCreateRequestBuilder withAttributes(@NonNull Map<String, String> attributes) {
        request.setAttributes(attributes);
        return this;
    }

    public FileCreateRequestBuilder withParentId(@NonNull String parentId) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(parentId));
        request.setParentId(parentId);
        return this;
    }

    public FileCreateRequestBuilder withType(@NonNull FileType type) {
        request.setType(type);
        return this;
    }

    public FileCreateResponse execute(List<String> params) throws Exception {
        FileCreateResponse response = new FileCreateResponse();
        response.setRequest(request);
        try {
            request.validate();
            if (path == null || !path.exists()) {
                throw new ValidationException("Path is not specified or file not found.");
            }
            FileHandle handle = client.upload(SERVICE_CONTENT_CREATE,
                    FileHandle.class,
                    request,
                    params,
                    SERVICE_FIELD_FILE,
                    SERVICE_FIELD_REQUEST,
                    path);
            response.setFileHandle(handle);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            response.setError(ex);
        }
        return response;
    }
}
