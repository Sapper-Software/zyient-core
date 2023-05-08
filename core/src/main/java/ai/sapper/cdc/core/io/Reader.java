package ai.sapper.cdc.core.io;

import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class Reader implements Closeable {
    private final FileInode inode;
    private final FileSystem fs;

    protected Reader(@NonNull FileInode inode,
                     @NonNull FileSystem fs) {
        this.inode = inode;
        this.fs = fs;
    }

    public abstract Reader open() throws IOException;

    public abstract int read(byte[] buffer, int offset, int length) throws IOException;

    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    public abstract void seek(int offset) throws IOException;

    public abstract boolean isOpen();

    public abstract File copy() throws IOException;
}
