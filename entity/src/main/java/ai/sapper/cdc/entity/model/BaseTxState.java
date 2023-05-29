package ai.sapper.cdc.entity.model;

import ai.sapper.cdc.core.state.EOffsetState;
import ai.sapper.cdc.core.state.OffsetState;
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
public class BaseTxState extends OffsetState<EOffsetState, BaseTxId> {

    public BaseTxState() {
        super(EOffsetState.Error, EOffsetState.Unknown);
    }
}
