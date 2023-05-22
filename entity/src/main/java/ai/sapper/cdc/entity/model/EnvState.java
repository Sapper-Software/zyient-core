package ai.sapper.cdc.entity.model;

import ai.sapper.cdc.common.AbstractEnvState;

public class EnvState extends AbstractEnvState<EEnvState> {
    public EnvState() {
        super(EEnvState.Error);
        setState(EEnvState.Unknown);
    }

    public boolean isAvailable() {
        return (getState() == EEnvState.Initialized);
    }
}
