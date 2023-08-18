package ai.sapper.cdc.intake.exceptions;

public class OnBehalfPermissionException extends Exception {
    public OnBehalfPermissionException (String exceptionMessage) {
        super(exceptionMessage);
    }
}
