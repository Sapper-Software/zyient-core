package ai.sapper.cdc.entity;

import ai.sapper.cdc.core.model.TransactionId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public class EntityState<T extends TransactionId> {
    private String domain;
    private String entity;
    private EEntityState state;
    private T loggedTxId;
    private T processedTxId;
    private T committedTxId;
    private long updatedTime;
    private String currentEditsPath;
    private String editsBasePath;
    private String zkPath;
    private String queue;
    private String errorQueue;
    private boolean snapshotCreated = false;
    private boolean snapshotCommitted = false;
    private long eventCount = 0;
    private long eventErrorCount = 0;
    private long editsEventCount = 0;
    private long committedEventCount = 0;

    public boolean canProcess() {
        return (state == EEntityState.ACTIVE || state == EEntityState.SNAPSHOT);
    }
}
