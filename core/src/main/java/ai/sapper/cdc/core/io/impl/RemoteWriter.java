package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.EFileState;
import ai.sapper.cdc.core.io.model.FileInode;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Getter
@Accessors(fluent = true)
public abstract class RemoteWriter extends Writer {
    private final RemoteFsCache cache;
    @Getter(AccessLevel.NONE)
    private File temp;
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
        try {
            DistributedLock lock = fs.getLock(inode);
            lock.lock();
            try {
                if (!Strings.isNullOrEmpty(inode.getTmpPath())) {
                    temp = new File(inode.getTmpPath());
                    if (!temp.exists()) {
                        throw new IOException(
                                String.format("Inode out of sync: local file not found. [path=%s]",
                                        temp.getAbsolutePath()));
                    }
                }
                if (temp == null) {
                    if (fs.exists(inode.getPathInfo())) {
                        temp = ((RemoteFileSystem) fs).download(inode);
                    } else {
                        temp = fs.createTmpFile(getTmpPath(), inode.getName());
                    }
                }
                outputStream = new FileOutputStream(temp, !overwrite);
                inode.setTmpPath(temp.getAbsolutePath());
                inode.getState().state(EFileState.Updating);

                inode = (FileInode) fs.updateInode(inode, inode.getPathInfo());
                lastFlushTimestamp = System.currentTimeMillis();
                lastFlushSize = fileSize();

                return this;
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
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

    public long fileSize() throws IOException {
        if (temp.exists()) {
            Path p = Paths.get(temp.toURI());
            return Files.size(p);
        }
        return 0;
    }

    private void checkUpload() throws IOException {
        long t = System.currentTimeMillis() - lastFlushTimestamp;
        long s = fileSize() - lastFlushSize;
        RemoteFileSystem rfs = (RemoteFileSystem) fs;
        RemoteFileSystem.RemoteFileSystemSettings rs = (RemoteFileSystem.RemoteFileSystemSettings) rfs.settings();
        if (t > rs.writerFlushInterval() || s > rs.writerFlushSize()) {
            commit();
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
    public void commit() throws IOException {
        try {
            File toUpload = temp;
            if (inode.isCompressed()) {
                toUpload = fs.compress(temp);
            }

            DistributedLock lock = fs.getLock(inode);
            lock.lock();
            try {
                canUpload();
                inode.setTmpPath(temp.getAbsolutePath());
                inode.setSyncedSize(inode.getPathInfo().dataSize());
                inode.getState().state(EFileState.PendingSync);

                fs.updateInode(inode, inode.getPathInfo());

                RemoteFileSystem rfs = (RemoteFileSystem) fs;
                rfs.upload(toUpload, inode);
            } finally {
                lock.unlock();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private void canUpload() throws Exception {
        FileInode current = (FileInode) fs.get(inode.getPathInfo());
        if (current == null) {
            throw new Exception(String.format("File Inode not found. [zkPath=%s]", inode.getZkPath()));
        }
        if (!current.getState().markedForUpdate()) {
            throw new Exception(
                    String.format("Inode not marked for update. [domain=%s][path=%s]",
                            inode.getDomain(), inode.getAbsolutePath()));
        }
        if (!Strings.isNullOrEmpty(current.getTmpPath())) {
            File tp = new File(current.getTmpPath());
            if (!temp.equals(tp)) {
                throw new Exception(
                        String.format("Cached file does not match. [expected=%s][local=%s]",
                                tp.getAbsolutePath(), temp.getAbsolutePath()));
            }
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
    }

    protected abstract String getTmpPath();
}
