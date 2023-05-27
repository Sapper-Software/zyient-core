package ai.sapper.cdc.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public abstract class AbstractEnvState<T extends Enum<?>> extends AbstractState<T> {

    public AbstractEnvState(@NonNull T errorState) {
        super(errorState);
    }

    @JsonIgnore
    public abstract boolean isAvailable();

    @JsonIgnore
    public abstract boolean isTerminated();
}
