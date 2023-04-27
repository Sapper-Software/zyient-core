package ai.sapper.cdc.core.io.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileInode extends Inode {
    private String tmpPath;
    private EFileState state = EFileState.Unknown;
    private long syncedSize = 0;
    private long updatedSize = 0;

    public FileInode(@NonNull String name) {
        super(InodeType.File, name);
    }

    @Override
    public boolean exists() {
        return (state != EFileState.Unknown && state != EFileState.Deleted);
    }
}
