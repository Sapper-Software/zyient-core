package ai.sapper.cdc.entity.model;

import ai.sapper.cdc.core.model.EngineType;
import ai.sapper.cdc.core.state.Offset;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class TransactionId extends Offset {
    private EngineType type;
    private long sequence = 0;
    private boolean snapshot;

    public TransactionId() {
    }

    public TransactionId(@NonNull TransactionId source) {
        this.type = source.type;
        this.sequence = source.sequence;
        this.snapshot = source.snapshot;
    }
}
