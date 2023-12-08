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

package io.zyient.core.filesystem.encryption;

import io.zyient.base.common.model.Context;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.filesystem.model.FileInode;
import lombok.NonNull;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.CompressionLevel;
import net.lingala.zip4j.model.enums.EncryptionMethod;

import java.io.File;
import java.io.IOException;

public class ProtectedZipHandler extends EncryptionHandler {
    public ProtectedZipHandler(@NonNull BaseEnv<?> env,
                               @NonNull String key) {
        super(env, key);
    }

    @Override
    public void encrypt(@NonNull File source,
                        @NonNull FileInode inode,
                        Context context,
                        @NonNull File outfile) throws IOException {
        if (outfile.exists()) {
            if (!outfile.delete()) {
                throw new IOException(
                        String.format("Failed to delete existing file. [path=%s]", outfile.getAbsolutePath()));
            }
        }
        if (!source.exists()) {
            throw new IOException(String.format("Source not found. [path=%s]", source.getAbsolutePath()));
        }
        ZipParameters parameters = new ZipParameters();
        parameters.setEncryptFiles(true);
        parameters.setCompressionLevel(CompressionLevel.HIGHER);
        parameters.setEncryptionMethod(EncryptionMethod.AES);

        if (source.isDirectory()) {
            try (ZipFile zip = new ZipFile(outfile.getAbsoluteFile(), key.toCharArray())) {
                zip.addFolder(source, parameters);
            }
        } else {
            try (ZipFile zip = new ZipFile(outfile.getAbsoluteFile(), key.toCharArray())) {
                zip.addFile(source, parameters);
            }
        }
    }

    @Override
    public void decrypt(@NonNull File source,
                        @NonNull FileInode inode,
                        Context context,
                        @NonNull File outfile) throws IOException {
        if (!outfile.exists() || !outfile.isDirectory()) {
            throw new IOException(
                    String.format("Output location not found or not directory. [path=%s]", outfile.getAbsolutePath()));
        }
        if (!source.exists()) {
            throw new IOException(String.format("Source file not found. [path=%s]", source.getAbsolutePath()));
        }
        try (ZipFile zip = new ZipFile(source.getAbsoluteFile(), key.toCharArray())) {
            zip.extractAll(outfile.getAbsolutePath());
        }
    }
}
