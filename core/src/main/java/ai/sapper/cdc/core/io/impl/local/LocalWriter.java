package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.EFileState;
import ai.sapper.cdc.core.io.model.FileInode;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

@Getter
@Accessors(fluent = true)
public class LocalWriter extends Writer {
    private FileOutputStream outputStream;
    private final LocalPathInfo path;

    protected LocalWriter(@NonNull FileInode inode,
                          @NonNull FileSystem fs,
                          boolean overwrite) throws IOException {
        super(inode, fs, overwrite);
        if (inode.getPathInfo() == null) {
            path = (LocalPathInfo) fs.parsePathInfo(inode.getPath());
            inode.setPathInfo(path);
        } else {
            path = (LocalPathInfo) inode.getPathInfo();
        }
    }

    /**
     * @param overwrite
     * @throws IOException
     */
    @Override
    public Writer open(boolean overwrite) throws IOException, DistributedLock.LockError {
        try (DistributedLock lock = fs.getLock(inode)) {
            lock.lock();
            try {
                inode = (FileInode) fs.fileLock(inode);
                checkLockCopy(overwrite);
                inode.getLock().setLocalPath(temp.getAbsolutePath());
                outputStream = new FileOutputStream(temp, !overwrite);
                inode.getState().setState(EFileState.Updating);

                inode = (FileInode) fs.updateInode(inode, path);

                return this;
            } finally {
                lock.unlock();
            }
        } catch (DistributedLock.LockError le) {
            String err = String.format("[%s][%s] %s", inode.getDomain(), inode.getAbsolutePath(), le.getLocalizedMessage());
            throw new DistributedLock.LockError(err);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    protected void getLocalCopy() throws Exception {
        if (path.file.exists()) {
            FileUtils.copyFile(path.file, temp);
            temp = Reader.checkDecompress(temp, inode, fs);
        }
    }

    /**
     * @param data
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public long write(byte[] data, long offset, long length) throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", path().toString()));
        }
        outputStream.write(data, (int) offset, (int) length);
        return length;
    }

    /**
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", path().toString()));
        }
        outputStream.flush();
    }

    /**
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public long truncate(long offset, long length) throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", path().toString()));
        }
        FileChannel channel = outputStream.getChannel();
        channel = channel.truncate(offset + length);
        return channel.size();
    }

    /**
     * @return
     */
    @Override
    public boolean isOpen() {
        return (outputStream != null);
    }

    @Override
    public void commit(boolean clearLock) throws IOException {
        try {
            File toUpload = temp;
            if (inode.isCompressed()) {
                toUpload = fs.compress(temp);
            }
            if (path.exists()) {
                if (!path.file.delete()) {
                    throw new IOException(
                            String.format("Failed to delete existing file. [path=%s]", path.file.getAbsolutePath()));
                }
                if (!toUpload.renameTo(path.file)) {
                    throw new IOException(
                            String.format("Failed to rename file. [path=%s]", toUpload.getAbsolutePath()));
                }
            }
            try (DistributedLock lock = fs.getLock(inode)) {
                lock.lock();
                try {
                    if (!fs.isFileLocked(inode)) {
                        throw new IOException(
                                String.format("[%s][%s] File not locked or locked by another process.",
                                        inode.getDomain(), inode.getAbsolutePath()));
                    }
                    String path = inode.getLock().getLocalPath();
                    if (path.compareTo(temp.getAbsolutePath()) == 0) {
                        throw new IOException(String.format("[%s][%s] Local path mismatch. [expected=%s][locked=%s]",
                                inode.getDomain(), inode.getAbsolutePath(),
                                temp.getAbsolutePath(), path));
                    }

                    inode.setSyncedSize(fileSize(temp));
                    inode.setSyncTimestamp(getLocalUpdateTime());
                    inode.getState().setState(EFileState.Synced);
                    if (clearLock) {
                        inode = (FileInode) fs.fileUnlock(inode);
                    } else {
                        inode = (FileInode) fs.fileUpdateLock(inode);
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
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
        if (outputStream != null) {
            try {
                outputStream.flush();
                outputStream.close();
                outputStream = null;
                if (temp.exists()) {
                    if (!temp.delete()) {
                        DefaultLogger.warn(
                                String.format("Failed to delete temporary file. [path=%s]", temp.getAbsolutePath()));
                    }
                }
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }
}
