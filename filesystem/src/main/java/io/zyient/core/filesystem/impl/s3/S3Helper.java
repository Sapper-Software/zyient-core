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

package io.zyient.core.filesystem.impl.s3;

import io.zyient.base.common.utils.PathUtils;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class S3Helper {
    public static File download(@NonNull S3Client client,
                                @NonNull String bucket,
                                @NonNull String path) throws IOException {
        String name = FilenameUtils.getName(path);
        File file = PathUtils.getTempFile(name);
        return download(client, bucket, path, file.getAbsolutePath());
    }

    public static File download(@NonNull S3Client client,
                                @NonNull String bucket,
                                @NonNull String path,
                                @NonNull String filepath) throws IOException {
        File file = new File(filepath);
        if (file.exists()) {
            if (!file.delete()) {
                throw new IOException(String.format("Failed to delete local file. [path=%s]",
                        file.getAbsolutePath()));
            }
        }
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            try (FileOutputStream fos = new FileOutputStream(file)) {
                client.getObject(request, ResponseTransformer.toOutputStream(fos));
            }
            return file;
        } catch (Throwable t) {
            throw new IOException(String.format("Download failed. [bucket=%s][path=%s]",
                    bucket, path), t);
        }
    }

    public static boolean exists(@NonNull S3Client client,
                                 @NonNull String bucket,
                                 @NonNull String path) throws IOException {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            HeadObjectResponse response = client.headObject(request);
            return (response != null);
        } catch (NoSuchKeyException nsk) {
            return false;
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    public static HeadObjectResponse upload(@NonNull S3Client client,
                                            @NonNull String bucket,
                                            @NonNull String path,
                                            @NonNull File source) throws IOException {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();
            PutObjectResponse response = client
                    .putObject(request, RequestBody.fromFile(source));
            S3Waiter waiter = client.waiter();
            HeadObjectRequest requestWait = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();

            WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);
            if (waiterResponse.matched().response().isEmpty()) {
                throw new Exception("Failed to get valid response...");
            }
            return waiterResponse.matched().response().get();
        } catch (Throwable t) {
            throw new IOException(String.format("Download failed. [bucket=%s][path=%s]",
                    bucket, path), t);
        }
    }

    public static HeadObjectResponse upload(@NonNull S3Client client,
                                            @NonNull String bucket,
                                            @NonNull String path,
                                            @NonNull String source,
                                             @NonNull String contentType) throws IOException {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .contentType(contentType)
                    .build();
            PutObjectResponse response = client
                    .putObject(request, RequestBody.fromString(source));
            S3Waiter waiter = client.waiter();
            HeadObjectRequest requestWait = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(path)
                    .build();

            WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);
            if (waiterResponse.matched().response().isEmpty()) {
                throw new Exception("Failed to get valid response...");
            }
            return waiterResponse.matched().response().get();
        } catch (Throwable t) {
            throw new IOException(String.format("Download failed. [bucket=%s][path=%s]",
                    bucket, path), t);
        }
    }
}
