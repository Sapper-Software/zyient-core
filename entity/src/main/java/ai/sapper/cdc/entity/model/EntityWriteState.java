package ai.sapper.cdc.entity.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class EntityWriteState<T extends TransactionId> extends EntityReadState<T> {
    private T committedTxId;
    private String currentEditsPath;
    private String editsBasePath;
    private boolean snapshotCommitted = false;
    private long committedEventCount = 0;

}
