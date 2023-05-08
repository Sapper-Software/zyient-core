package ai.sapper.cdc.core.io.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public abstract class Inode {
    private String uuid;
    private Map<String, String> path;
    private String absolutePath;
    private long createTimestamp = 0;
    private long updateTimestamp = 0;
    private long syncTimestamp = 0;
    private InodeType type;
    private Inode parent = null;
    private String name;
    private String zkPath;

    public Inode() {

    }

    public Inode(@NonNull InodeType type,
                 @NonNull String name) {
        uuid = UUID.randomUUID().toString();
        this.type = type;
        this.name = name;
    }

    public boolean isDirectory() {
        return (type == InodeType.Directory);
    }

    public boolean isFile() {
        return (type == InodeType.File);
    }

    public boolean isArchive() {
        return (type == InodeType.Archive);
    }

    public long size() throws IOException {
        return path.size();
    }
}
