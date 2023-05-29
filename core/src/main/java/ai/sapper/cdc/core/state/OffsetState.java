package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.core.model.ModuleInstance;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public abstract class OffsetState<T extends Enum<?>, O extends Offset> extends AbstractState<T> {
    private String type;
    private String name;
    private O offset;
    private long timeCreated = 0;
    private long timeUpdated = 0;
    private ModuleInstance lastUpdatedBy;

    public OffsetState(@NonNull T errorState,
                       @NonNull T initState) {
        super(errorState, initState);
    }
}
