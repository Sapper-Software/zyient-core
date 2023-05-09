package ai.sapper.cdc.core.io;

import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class Writer implements Closeable {
    private final FileInode inode;
    private final FileSystem<?> fs;
    private final boolean overwrite;

    protected Writer(@NonNull FileInode inode,
                     @NonNull FileSystem<?> fs,
                     boolean overwrite) {
        this.inode = inode;
        this.fs = fs;
        this.overwrite = overwrite;
    }

    public abstract Writer open(boolean overwrite) throws IOException;

    public Writer open() throws IOException {
        return open(overwrite);
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
}
