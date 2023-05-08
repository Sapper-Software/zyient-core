package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.impl.local.LocalReader;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;

public class AzureReader extends LocalReader {
    private final AzureFileSystem fs;

    public AzureReader(@NonNull AzureFileSystem fs,
                       @NonNull PathInfo path) {
        super(path);
        this.fs = fs;
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public Reader open() throws IOException {
        AzurePathInfo path = AzureFileSystem.checkPath(inode());
        fs.read(path);

        return super.open();
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
        return super.read(buffer, offset, length);
    }

    @Override
    public File copy() throws IOException {
        AzurePathInfo path = AzureFileSystem.checkPath(inode());
        if (path.temp() != null && path.temp().exists()) {
            return path.temp();
        } else {
            fs.read(path);
            return path.temp();
        }
    }

    /**
     * @param offset
     * @throws IOException
     */
    @Override
    public void seek(int offset) throws IOException {
        super.seek(offset);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        super.close();
    }
}
