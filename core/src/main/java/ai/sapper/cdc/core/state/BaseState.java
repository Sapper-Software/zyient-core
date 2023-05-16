package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.AbstractState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public abstract class BaseState<T extends Enum<?>, O extends Offset> extends AbstractState<T> {
    private O offset;
    private long timeCreated = 0;
    private long timeUpdated = 0;

    public BaseState(@NonNull T errorState) {
        super(errorState);
    }
}
