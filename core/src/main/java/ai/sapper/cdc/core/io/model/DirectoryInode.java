package ai.sapper.cdc.core.io.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DirectoryInode extends Inode {
    @JsonIgnore
    private boolean synced = false;

    public DirectoryInode() {
    }

    public DirectoryInode(@NonNull String name) {
        super(InodeType.Directory, name);
    }
}
