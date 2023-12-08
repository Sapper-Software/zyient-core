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
import org.apache.commons.io.FilenameUtils;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.encryption.AccessPermission;
import org.apache.pdfbox.pdmodel.encryption.StandardProtectionPolicy;

import java.io.File;
import java.io.IOException;

public class ProtectedPDFHandler extends EncryptionHandler {
    public static final String CONTEXT_KEY_PERMISSIONS = "PDF_PERMISSIONS";

    public ProtectedPDFHandler(@NonNull BaseEnv<?> env,
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
        try (PDDocument doc = Loader.loadPDF(source)) {
            AccessPermission perm = null;
            if (context != null && context.containsKey(CONTEXT_KEY_PERMISSIONS)) {
                perm = (AccessPermission) context.get(CONTEXT_KEY_PERMISSIONS);
            }
            if (perm == null) {
                perm = new AccessPermission();
            }
            StandardProtectionPolicy spp = new StandardProtectionPolicy(key, key, perm);
            spp.setEncryptionKeyLength(128);
            spp.setPermissions(perm);
            doc.protect(spp);
            doc.save(outfile);
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
        String filename = FilenameUtils.getName(source.getAbsolutePath());
        File outf = new File(String.format("%s/%s", outfile.getAbsolutePath(), filename));
        if (outf.exists()) {
            if (!outf.delete()) {
                throw new IOException(
                        String.format("Failed to delete existing file. [path=%s]", outf.getAbsolutePath()));
            }
        }
        try (PDDocument doc = Loader.loadPDF(source, key)) {
            doc.setAllSecurityToBeRemoved(true);
            doc.save(outf);
        }
    }
}
