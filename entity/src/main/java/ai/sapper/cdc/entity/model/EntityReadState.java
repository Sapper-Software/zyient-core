package ai.sapper.cdc.entity.model;

import ai.sapper.cdc.core.state.EOffsetState;
import ai.sapper.cdc.core.state.OffsetState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class EntityReadState<T extends TransactionId> extends OffsetState<EEntityState, T> {
    private String domain;
    private String entity;
    private T processedTxId;
    private String zkPath;
    private String queue;
    private String errorQueue;
    private long eventCount = 0;
    private long eventErrorCount = 0;
    private long editsEventCount = 0;

    public EntityReadState() {
        super(EEntityState.ERROR);
    }

    public boolean canProcess() {
        return (getState() == EEntityState.ACTIVE || getState() == EEntityState.SNAPSHOT);
    }
}
