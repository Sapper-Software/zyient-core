package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public abstract class RemoteWriter extends Writer {
    private final RemoteFsCache cache;

    protected RemoteWriter(@NonNull FileInode inode,
                           @NonNull RemoteFileSystem fs,
                           boolean overwrite) {
        super(inode, fs, overwrite);
        cache = fs.cache();
    }
}
