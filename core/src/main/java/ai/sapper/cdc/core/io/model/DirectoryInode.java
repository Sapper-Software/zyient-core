package ai.sapper.cdc.core.io.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DirectoryInode extends Inode {
    @JsonIgnore
    private boolean synced = false;
    private Map<String, String> archivePath;

    public DirectoryInode() {
    }

    public DirectoryInode(@NonNull String domain,
                          @NonNull String name) {
        super(InodeType.Directory, domain, name);
    }
}
