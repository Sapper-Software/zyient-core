package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.core.io.model.FileInode;
import lombok.NonNull;

public interface FileUploadCallback {
    void onSuccess(@NonNull FileInode inode, @NonNull Object response, boolean clearLock);

    void onError(@NonNull FileInode inode, @NonNull Throwable error);
}
