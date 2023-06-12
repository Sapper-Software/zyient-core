package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.EFileState;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

@Getter
@Accessors(fluent = true)
public abstract class RemoteWriter extends Writer {
    private final RemoteFsCache cache;
    private FileOutputStream outputStream;
    private long lastFlushTimestamp;
    private long lastFlushSize;

    protected RemoteWriter(@NonNull FileInode inode,
                           @NonNull RemoteFileSystem fs,
                           boolean overwrite) {
        super(inode, fs, overwrite);
        cache = fs.cache();
    }

    @Override
    public Writer open(boolean overwrite) throws IOException {
        try (DistributedLock lock = fs.getLock(inode)) {
            lock.lock();
            try {
                inode = (FileInode) fs.fileLock(inode);
                checkLocalCopy(overwrite);
                inode.getLock().setLocalPath(temp.getAbsolutePath());
                outputStream = new FileOutputStream(temp, !overwrite);
                inode.getState().setState(EFileState.Updating);

                inode = (FileInode) fs.updateInode(inode);
                lastFlushTimestamp = System.currentTimeMillis();
                lastFlushSize = fileSize(temp);

                return this;
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    protected void getLocalCopy() throws Exception {
        if (fs.exists(inode.getPathInfo())) {
            File file = fs.download(inode);
            if (file == null) return;
            if (inode.isCompressed()) {
                File outf = fs.decompress(file);
                if (!file.delete()) {
                    throw new IOException(String.format("Failed to delete file. [path=%s]", file.getAbsolutePath()));
                }
                if (!outf.renameTo(file)) {
                    throw new IOException(String.format("Filed to rename file. [path=%s]", outf.getAbsolutePath()));
                }
            }
            temp = file;
        }
    }

    @Override
    public long write(byte[] data) throws IOException {
        return write(data, 0, data.length);
    }

    @Override
    public long write(byte[] data, long length) throws IOException {
        return write(data, 0, length);
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
            throw new IOException(String.format("Writer not open: [path=%s]", inode));
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
            throw new IOException(String.format("Writer not open: [path=%s]", inode));
        }
        outputStream.flush();
        checkUpload();
    }


    private void checkUpload() throws IOException {
        long t = System.currentTimeMillis() - lastFlushTimestamp;
        long s = fileSize(temp) - lastFlushSize;
        RemoteFileSystem rfs = (RemoteFileSystem) fs;
        RemoteFileSystemSettings rs = (RemoteFileSystemSettings) rfs.settings();
        if (t > rs.getWriterFlushInterval() || s > rs.getWriterFlushSize()) {
            commit(false);
        }
    }


    @Override
    public long truncate(int length) throws IOException {
        return truncate(0, length);
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
            throw new IOException(String.format("Writer not open: [path=%s]", inode.toString()));
        }
        FileChannel channel = outputStream.getChannel();
        channel = channel.truncate(offset + length);
        return channel.size();
    }

    @Override
    public Writer open() throws IOException {
        return open(overwrite());
    }

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

            try (DistributedLock lock = fs.getLock(inode)) {
                lock.lock();
                try {
                    if (!fs.isFileLocked(inode)) {
                        throw new IOException(
                                String.format("[%s][%s] File not locked or locked by another process.",
                                        inode.getDomain(), inode.getAbsolutePath()));
                    }
                    String path = inode.getLock().getLocalPath();
                    if (path.compareTo(temp.getAbsolutePath()) != 0) {
                        throw new IOException(String.format("[%s][%s] Local path mismatch. [expected=%s][locked=%s]",
                                inode.getDomain(), inode.getAbsolutePath(),
                                temp.getAbsolutePath(), path));
                    }
                    inode.setSyncedSize(fileSize(temp));
                    inode.setSyncTimestamp(getLocalUpdateTime());
                    inode.getState().setState(EFileState.PendingSync);

                    inode = (FileInode) fs.updateInode(inode);

                    RemoteFileSystem rfs = (RemoteFileSystem) fs;
                    inode = rfs.upload(toUpload, inode, clearLock);
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }


    @Override
    public void close() throws IOException {
        if (isOpen()) {
            flush();
        }
        if (outputStream != null) {
            outputStream.close();
            outputStream = null;
        }
        if (temp != null && temp.exists()) {
            if (!temp.delete()) {
                DefaultLogger.warn(String.format("Failed to delete local copy. [path=%s]", temp.getAbsolutePath()));
            }
        }
    }

    @Override
    public OutputStream getOutputStream() throws Exception {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", inode.toString()));
        }
        return outputStream;
    }

    protected abstract String getTmpPath();
}
