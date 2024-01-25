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

package io.zyient.core.filesystem.encryption;

import io.zyient.base.common.GlobalConstants;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.CypherUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.filesystem.model.FileInode;
import lombok.NonNull;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class AESEncryptionHandler extends EncryptionHandler {
    private static final String CONTEXT_KEY_ALGO = "CIPHER_AES_TYPE";
    private static final String CONTEXT_KEY_IV = "CIPHER_AES_IV";
    private static final int BUFFER_SIZE = 1024 * 1024 * 32;

    private String ivSpec;

    public AESEncryptionHandler(@NonNull BaseEnv<?> env,
                                @NonNull String key,
                                @NonNull String ivSpec) {
        super(env, key);
        this.ivSpec = ivSpec;
    }

    @Override
    public void encrypt(@NonNull File source,
                        @NonNull FileInode inode,
                        Context context,
                        @NonNull File outfile) throws IOException {
        try {
            Cipher cipher = getCipher(context, Cipher.ENCRYPT_MODE);
            byte[] iv = cipher.getIV();
            try (FileOutputStream fos = new FileOutputStream(outfile)) {
                try (CipherOutputStream cos = new CipherOutputStream(fos, cipher)) {
                    fos.write(iv);
                    try (FileInputStream fis = new FileInputStream(source)) {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        while (true) {
                            int r = fis.read(buffer);
                            if (r <= 0) break;
                            cos.write(buffer, 0, r);
                            if (r < BUFFER_SIZE) break;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void decrypt(@NonNull File source,
                        @NonNull FileInode inode,
                        Context context,
                        @NonNull File outfile) throws IOException {
        try {
            try (FileInputStream fis = new FileInputStream(source)) {
                byte[] fileIv = new byte[16];
                int r = fis.read(fileIv);
                if (r < 16) {
                    throw new IOException(
                            String.format("Failed to read IV from file. [file=%s]", source.getAbsolutePath()));
                }
                if (context == null) {
                    context = new Context();
                }
                context.put(CONTEXT_KEY_IV, new String(fileIv, GlobalConstants.defaultCharset()));
                Cipher cipher = getCipher(context, Cipher.DECRYPT_MODE);
                try (CipherInputStream cis = new CipherInputStream(fis, cipher)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    try (FileOutputStream fos = new FileOutputStream(outfile)) {
                        while (true) {
                            r = cis.read(buffer);
                            if (r <= 0) break;
                            fos.write(buffer, 0, r);
                            if (r < BUFFER_SIZE) break;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private Cipher getCipher(Context context, int mode) throws Exception {
        if (context != null) {
            String algo = CypherUtils.CIPHER_ALGO;
            String iv = ivSpec;
            if (context.containsKey(CONTEXT_KEY_ALGO)) {
                algo = (String) context.get(CONTEXT_KEY_ALGO);
            }
            if (context.containsKey(CONTEXT_KEY_IV)) {
                iv = (String) context.get(CONTEXT_KEY_IV);
            }
            return CypherUtils.getCipher(algo, key, iv, mode);
        }
        return CypherUtils.getCipher(key, ivSpec, mode);
    }
}
