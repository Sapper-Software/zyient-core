package ai.sapper.cdc.entity;

import ai.sapper.cdc.common.AbstractEnvState;

public class EnvState extends AbstractEnvState<EEnvState> {
    public EnvState() {
        super(EEnvState.Error);
        state(EEnvState.Unknown);
    }

    public boolean isAvailable() {
        return (state() == EEnvState.Initialized);
    }
}
