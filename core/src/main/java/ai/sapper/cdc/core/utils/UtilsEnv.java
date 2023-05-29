package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.AbstractEnvState;
import ai.sapper.cdc.core.BaseEnv;
import lombok.NonNull;

public class UtilsEnv extends BaseEnv<UtilsEnv.EUtilsState> {
    public enum EUtilsState {
        Unknown, Available, Disposed, Error
    }

    public static class UtilsState extends AbstractEnvState<EUtilsState> {

        public UtilsState() {
            super(EUtilsState.Error, EUtilsState.Unknown);
        }

        @Override
        public boolean isAvailable() {
            return getState() == EUtilsState.Available;
        }

        @Override
        public boolean isTerminated() {
            return (getState() == EUtilsState.Disposed || hasError());
        }
    }

    public UtilsEnv(@NonNull String name) {
        super(name);
    }
}
