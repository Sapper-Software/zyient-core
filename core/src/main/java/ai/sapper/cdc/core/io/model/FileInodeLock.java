package ai.sapper.cdc.core.io.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class FileInodeLock {
    private String clientId;
    private String fs;
    private String localPath;
    private long timeLocked;
    private long timeUpdated;

    public FileInodeLock() {

    }

    public FileInodeLock(@NonNull String clientId,
                         @NonNull String fs) {
        this.clientId = clientId;
        this.fs = fs;
        this.timeLocked = System.currentTimeMillis();
    }
}
