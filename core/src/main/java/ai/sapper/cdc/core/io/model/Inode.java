package ai.sapper.cdc.core.io.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public abstract class Inode {
    private String uuid;
    private PathInfo path;
    private long createTimestamp = 0;
    private long updateTimestamp = 0;
    private long syncTimestamp = 0;
    private InodeType type;
    private Inode parent = null;
    private String name;

    public Inode(@NonNull InodeType type,
                 @NonNull String name) {
        uuid = UUID.randomUUID().toString();
        this.type = type;
        this.name = name;
    }

    public abstract boolean exists();
}
