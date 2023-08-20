package ai.sapper.cdc.intake.model;

import ai.sapper.cdc.common.AbstractState;

public class RecordState extends AbstractState<ERecordState> {

    public RecordState() {
        super(ERecordState.Unknown, ERecordState.Error);
    }
}
