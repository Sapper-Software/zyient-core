package ai.sapper.cdc.entity.model;

import ai.sapper.cdc.common.AbstractEnvState;

public class EnvState extends AbstractEnvState<EEnvState> {
    public EnvState() {
        super(EEnvState.Error, EEnvState.Unknown);
        setState(EEnvState.Unknown);
    }

    public boolean isAvailable() {
        return (getState() == EEnvState.Initialized);
    }

    @Override
    public boolean isTerminated() {
        return (getState() == EEnvState.Disposed || hasError());
    }
}
