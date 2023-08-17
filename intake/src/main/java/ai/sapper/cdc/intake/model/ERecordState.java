package ai.sapper.cdc.intake.model;

import com.codekutter.common.IState;

public enum ERecordState implements IState<ERecordState> {
    Unknown, Pending, Processing, IngestProcessed, IntakeProcessing, IntakeProcessed,Error;
    @Override
    public ERecordState getErrorState() {
        return Error;
    }
}
