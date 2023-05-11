package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.model.FileInode;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

@Getter
@Accessors(fluent = true)
public abstract class RemoteReader extends Reader {
    private final RemoteFsCache cache;
    private File cacheFile;
    private RandomAccessFile inputStream;

    protected RemoteReader(@NonNull FileInode inode,
                           @NonNull RemoteFileSystem fs) {
        super(inode, fs);
        cache = fs.cache();
    }

    public RemoteReader open() throws IOException {
        try {
            cacheFile = cache.get(inode());
            if (cacheFile == null || !cacheFile.exists()) {
                throw new IOException(
                        String.format("Error downloading file to local. [path=%s]", inode.getAbsolutePath()));
            }

            inputStream = new RandomAccessFile(cacheFile, "r");
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }


    /**
     * @param buffer
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
        }
        return inputStream.read(buffer, offset, length);
    }

    /**
     * @param offset
     * @throws IOException
     */
    @Override
    public void seek(int offset) throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
        }
        inputStream.seek(offset);
    }

    /**
     * @return
     */
    @Override
    public boolean isOpen() {
        return (inputStream != null);
    }

    @Override
    public File copy() throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode().toString()));
        }
        return cacheFile;
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
        cacheFile = null;
        inputStream = null;
    }
}
