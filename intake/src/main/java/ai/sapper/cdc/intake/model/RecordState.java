package ai.sapper.cdc.intake.model;


import com.codekutter.common.AbstractState;

public class RecordState extends AbstractState<ERecordState> {

    public RecordState() {
        setState(ERecordState.Unknown);
    }
}
