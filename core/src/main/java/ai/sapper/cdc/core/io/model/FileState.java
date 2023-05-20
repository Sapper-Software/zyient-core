package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.common.AbstractState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class FileState extends AbstractState<EFileState> {
    public FileState() {
        super(EFileState.Error);
    }

    public boolean markedForUpdate() {
        return (getState() == EFileState.Updating
                || getState() == EFileState.PendingSync
                || getState() == EFileState.New);
    }

    public boolean synced() {
        return getState() == EFileState.Synced;
    }
}
