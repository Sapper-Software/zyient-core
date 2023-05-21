package ai.sapper.cdc.core.io;

import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.io.model.FileInode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

@Getter
@Accessors(fluent = true)
public abstract class Writer implements Closeable {
    protected FileInode inode;
    protected final FileSystem fs;
    private final boolean overwrite;
    @Getter(AccessLevel.PROTECTED)
    protected File temp;

    protected Writer(@NonNull FileInode inode,
                     @NonNull FileSystem fs,
                     boolean overwrite) {
        Preconditions.checkArgument(inode.getPathInfo() != null);
        this.inode = inode;
        this.fs = fs;
        this.overwrite = overwrite;
    }

    public abstract Writer open(boolean overwrite) throws IOException, DistributedLock.LockError;

    public Writer open() throws IOException {
        return open(overwrite);
    }

    public long fileSize(@NonNull File temp) throws IOException {
        if (temp.exists()) {
            Path p = Paths.get(temp.toURI());
            return Files.size(p);
        }
        return 0;
    }

    public abstract long write(byte[] data, long offset, long length) throws IOException;

    public long write(byte[] data) throws IOException {
        return write(data, 0, data.length);
    }

    public long write(byte[] data, long length) throws IOException {
        return write(data, 0, length);
    }

    public abstract void flush() throws IOException;

    public abstract long truncate(long offset, long length) throws IOException;

    public long truncate(int length) throws IOException {
        return truncate(0, length);
    }

    public abstract boolean isOpen();

    public abstract void commit(boolean clearLock) throws IOException;


    protected void checkLockCopy(boolean overwrite) throws Exception {
        if (!Strings.isNullOrEmpty(inode.getLock().getLocalPath())) {
            temp = new File(inode.getLock().getLocalPath());
        } else {
            temp = fs.createTmpFile(null, inode.getName());
        }
        if (overwrite) {
            if (temp.exists()) {
                if (!temp.delete()) {
                    throw new IOException(
                            String.format("Failed to delete temp file. [path=%s]", temp.getAbsolutePath()));
                }
            }
        } else {
            if (temp.exists()) {
                if (fs.exists(inode.getPathInfo())) {
                    long uts = getLocalUpdateTime();
                    if (uts < inode.getSyncTimestamp()) {
                        throw new IOException(String.format("Local copy is stale. [inode=%s]", inode.toString()));
                    }
                }
            } else {
                getLocalCopy();
            }
        }
    }

    protected long getLocalUpdateTime() throws IOException {
        return getLocalUpdateTime(temp);
    }

    private long getLocalUpdateTime(File file) throws IOException {
        if (file.exists()) {
            Path p = Paths.get(file.toURI());
            BasicFileAttributes attr =
                    Files.readAttributes(p, BasicFileAttributes.class);
            FileTime ft = attr.lastModifiedTime();
            return ft.toMillis();
        }
        return 0;
    }

    protected abstract void getLocalCopy() throws Exception;
}
