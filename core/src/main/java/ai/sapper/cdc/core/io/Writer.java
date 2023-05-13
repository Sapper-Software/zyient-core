package ai.sapper.cdc.core.io;

import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.io.model.FileInode;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Getter
@Accessors(fluent = true)
public abstract class Writer implements Closeable {
    protected FileInode inode;
    protected final FileSystem fs;
    private final boolean overwrite;

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
}
