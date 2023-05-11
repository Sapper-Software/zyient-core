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
    private FileState state = new FileState();
    private boolean compressed = false;
    private long syncedSize = 0;
    private long updatedSize = 0;

    public FileInode() {

    }

    public FileInode(@NonNull String domain,
                     @NonNull String name) {
        super(InodeType.File, domain, name);
    }
}
