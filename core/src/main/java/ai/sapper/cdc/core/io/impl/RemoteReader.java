package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public abstract class RemoteReader extends Reader {
    private final RemoteFsCache cache;

    protected RemoteReader(@NonNull FileInode inode,
                           @NonNull RemoteFileSystem fs) {
        super(inode, fs);
        cache = fs.cache();
    }
}
