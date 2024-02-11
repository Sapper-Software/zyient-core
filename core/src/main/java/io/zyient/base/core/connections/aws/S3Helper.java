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

package io.zyient.base.core.connections.aws;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.utils.FSPathUtils;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class S3Helper {
    public static final int DEFAULT_RETRY_COUNT = 5;

    public static File download(@NonNull S3Client client,
                                @NonNull String bucket,
                                @NonNull String path) throws IOException {
        return download(client, bucket, path, DEFAULT_RETRY_COUNT);
    }

    public static File download(@NonNull S3Client client,
                                @NonNull String bucket,
                                @NonNull String path,
                                int retryCount) throws IOException {
        String name = FilenameUtils.getName(path);
        File file = PathUtils.getTempFile(name);
        return download(client, bucket, path, file.getAbsolutePath(), retryCount);
    }

    public static File download(@NonNull S3Client client,
                                @NonNull String bucket,
                                @NonNull String path,
                                @NonNull String filepath,
                                int retryCount) throws IOException {
        File file = new File(filepath);
        if (file.exists()) {
            if (!file.delete()) {
                throw new IOException(String.format("Failed to delete local file. [path=%s]",
                        file.getAbsolutePath()));
            }
        }
        path = FSPathUtils.encode(path);
        RunUtils.BackoffWait wait = RunUtils.create(retryCount);
        while (true) {
            try {
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(path)
                        .build();
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    ResponseBytes<GetObjectResponse> objectBytes = client.getObjectAsBytes(request);
                    byte[] data = objectBytes.asByteArray();
                    fos.write(data);
                    // client.getObject(request, ResponseTransformer.toOutputStream(fos));
                }
                return file;
            } catch (NoSuchKeyException nk) {
                if (wait.check()) {
                    DefaultLogger.warn(String.format("Waiting for file. [bucket=%s][path=%s]",
                            bucket, path));
                    continue;
                }
                throw new IOException(String.format("File not found: [bucket=%s][key=%s][retries=%d]",
                        bucket, path, retryCount));
            } catch (Throwable t) {
                throw new IOException(String.format("Download failed. [bucket=%s][path=%s]",
                        bucket, path), t);
            }
        }
    }

    public static File download(@NonNull S3Client client,
                                @NonNull String bucket,
                                @NonNull String path,
                                @NonNull String filepath) throws IOException {
        return download(client, bucket, path, filepath, DEFAULT_RETRY_COUNT);
    }

    public static boolean exists(@NonNull S3Client client,
                                 @NonNull String bucket,
                                 @NonNull String path) throws IOException {
        path = FSPathUtils.encode(path);
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

    public static void delete(@NonNull S3Client client,
                              @NonNull String bucket,
                              @NonNull String path,
                              boolean recursive) throws IOException {
        try {
            path = FSPathUtils.encode(path);
            if (recursive) {
                ListObjectsRequest request = ListObjectsRequest
                        .builder()
                        .bucket(bucket)
                        .prefix(path)
                        .build();

                ListObjectsResponse res = client.listObjects(request);
                List<S3Object> objects = res.contents();
                for (S3Object obj : objects) {
                    DeleteObjectRequest dr = DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(obj.key())
                            .build();
                    DeleteObjectResponse r = client.deleteObject(dr);
                    SdkHttpResponse sr = r.sdkHttpResponse();
                    if (sr.statusCode() < 200 || sr.statusCode() >= 300) {
                        String mesg = JSONUtils.asString(sr);
                        throw new IOException(mesg);
                    } else {
                        DefaultLogger.info(String.format("[bucket=%s] Deleted path: %s", bucket, path));
                    }
                }
            } else {
                DeleteObjectRequest dr = DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(path)
                        .build();
                DeleteObjectResponse r = client.deleteObject(dr);
                SdkHttpResponse sr = r.sdkHttpResponse();
                if (sr.statusCode() >= 200 && sr.statusCode() < 300) {
                    DefaultLogger.info(String.format("[bucket=%s] Deleted file: %s", bucket, path));
                } else {
                    String mesg = JSONUtils.asString(sr);
                    throw new IOException(mesg);
                }
            }
        } catch (Throwable t) {
            throw new IOException(String.format("Delete failed. [bucket=%s][path=%s]",
                    bucket, path), t);
        }
    }

    public static HeadObjectResponse upload(@NonNull S3Client client,
                                            @NonNull String bucket,
                                            @NonNull String path,
                                            @NonNull File source) throws IOException {
        try {
            path = FSPathUtils.encode(path);
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
            throw new IOException(String.format("Upload failed. [bucket=%s][path=%s]",
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
