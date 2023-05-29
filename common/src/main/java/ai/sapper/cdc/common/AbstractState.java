package ai.sapper.cdc.common;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public abstract class AbstractState<T extends Enum<?>> {
    private T state;
    @Setter(AccessLevel.NONE)
    private Throwable error;
    private final T errorState;
    private final T initState;

    public AbstractState(@NonNull T errorState,
                         @NonNull T initState) {
        this.errorState = errorState;
        this.initState = initState;
    }

    public AbstractState<T> error(@NonNull Throwable error) {
        state = errorState;
        this.error = error;
        return this;
    }

    public boolean hasError() {
        return (state == errorState);
    }

    public void clear() {
        state = initState;
        error = null;
    }
}
