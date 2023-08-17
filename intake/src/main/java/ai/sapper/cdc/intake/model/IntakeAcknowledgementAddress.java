package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IntakeAcknowledgementAddress {
    private String intakeAcknowledgementAddress;

    public IntakeAcknowledgementAddress(){

    }
    public IntakeAcknowledgementAddress(String intakeAcknowledgementAddress){
        this.setIntakeAcknowledgementAddress(intakeAcknowledgementAddress);
    }
}
