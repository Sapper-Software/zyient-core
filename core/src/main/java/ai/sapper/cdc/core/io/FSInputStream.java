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

package ai.sapper.cdc.core.io;

import lombok.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FSInputStream extends InputStream {
    private final Reader reader;

    public FSInputStream(@NonNull Reader reader) {
        this.reader = reader;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Method not implemented...");
    }

    @Override
    public int read(byte[] b) throws IOException {
        return reader.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return reader.read(b, off, len);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        throw new IOException("Method not supported...");
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        byte[] buffer = new byte[len];
        reader.read(buffer);
        return buffer;
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return reader.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        throw new IOException("Method not supported...");
    }

    @Override
    public int available() throws IOException {
        throw new IOException("Method not supported...");
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new RuntimeException("Method not supported...");
    }

    @Override
    public synchronized void reset() throws IOException {
        reader.seek(0);
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        long bytes = 0;
        reader.seek(0);
        int bsize = 8096;
        byte[] buffer = new byte[bsize];
        while (true) {
            int r = read(buffer);
            if (r <= 0) break;
            out.write(buffer, 0, r);
            if (r < bsize) break;
        }
        out.flush();
        return bytes;
    }
}
