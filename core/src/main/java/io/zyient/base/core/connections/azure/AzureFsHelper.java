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

package io.zyient.base.core.connections.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.utils.FSPathUtils;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;

public class AzureFsHelper {
    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final String DELIMITER = "/";

    public static File download(@NonNull BlobServiceClient client,
                                @NonNull String container,
                                @NonNull String path) throws IOException {
        return download(client, container, path, DEFAULT_RETRY_COUNT);
    }

    public static File download(@NonNull BlobServiceClient client,
                                @NonNull String container,
                                @NonNull String path,
                                int retryCount) throws IOException {
        String name = FilenameUtils.getName(path);
        File file = PathUtils.getTempFile(name);
        return download(client, container, path, file.getAbsolutePath(), retryCount);
    }

    public static File download(@NonNull BlobServiceClient client,
                                @NonNull String container,
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
                BlobContainerClient cc = client.getBlobContainerClient(container);
                if (cc.exists()) {
                    BlobClient bc = cc.getBlobClient(path);
                    if (bc.exists()) {
                        try (FileOutputStream fos = new FileOutputStream(file)) {
                            bc.downloadStream(fos);
                        }
                    } else {
                        if (wait.check()) {
                            DefaultLogger.warn(String.format("Waiting for file. [container=%s][path=%s]",
                                    container, path));
                            continue;
                        }
                        throw new IOException(String.format("File not found: [container=%s][key=%s][retries=%d]",
                                container, path, retryCount));
                    }
                } else {
                    throw new IOException(String.format("Container not found. [container=%s]", container));
                }
                return file;
            } catch (Throwable t) {
                throw new IOException(String.format("Download failed. [container=%s][path=%s]",
                        container, path), t);
            }
        }
    }

    public static boolean exists(@NonNull BlobServiceClient client,
                                 @NonNull String container,
                                 @NonNull String path) throws IOException {
        path = FSPathUtils.encode(path);
        try {
            BlobContainerClient cc = client.getBlobContainerClient(container);
            if (cc.exists()) {
                return cc.getBlobClient(path).exists();
            }
            return false;
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    public static boolean delete(@NonNull BlobServiceClient client,
                                 @NonNull String container,
                                 boolean hierarchical,
                                 @NonNull String path,
                                 boolean recursive) throws IOException {
        BlobContainerClient cc = client.getBlobContainerClient(container);
        if (cc.exists()) {
            if (!hierarchical) {
                BlobClient bc = cc.getBlobClient(path);
                if (bc.exists()) {
                    DefaultLogger.trace(String.format("Deleting Azure BLOB: [name=%s]", bc.getBlobName()));
                    bc.delete();
                    return true;
                }
            } else {
                boolean ret = false;
                ListBlobsOptions options = new ListBlobsOptions()
                        .setPrefix(path);
                Iterable<BlobItem> blobs = cc.listBlobsByHierarchy(DELIMITER, options, null);
                for (BlobItem bi : blobs) {
                    String name = bi.getName();
                    if (bi.isDeleted()) continue;
                    if (!recursive) {
                        if (name.compareTo(path) != 0) {
                            continue;
                        }
                    } else if (bi.isPrefix()) {
                        ret = true;
                        continue;
                    }
                    BlobClient bc = cc.getBlobClient(name);
                    if (bc.exists()) {
                        DefaultLogger.trace(String.format("Deleting Azure BLOB: [name=%s]", name));
                        bc.delete();
                        ret = true;
                    }
                }
                return ret;
            }
        }
        return false;
    }

    public static Object upload(@NonNull BlobServiceClient client,
                                @NonNull String container,
                                @NonNull String path,
                                long uploadTimeout,
                                @NonNull File source) throws IOException {
        try {
            path = FSPathUtils.encode(path);
            BlobContainerClient cc = client.getBlobContainerClient(container);
            if (!cc.exists()) {
                throw new IOException(String.format("Azure Container not found. [container=%s]", container));
            }
            BlobUploadFromFileOptions options = new BlobUploadFromFileOptions(source.getAbsolutePath());
            Duration timeout = Duration.ofSeconds(uploadTimeout);
            BlobClient bc = cc.getBlobClient(path);
            return bc.uploadFromFileWithResponse(options, timeout, null);
        } catch (Throwable t) {
            throw new IOException(String.format("Upload failed. [bucket=%s][path=%s]",
                    container, path), t);
        }
    }

}
