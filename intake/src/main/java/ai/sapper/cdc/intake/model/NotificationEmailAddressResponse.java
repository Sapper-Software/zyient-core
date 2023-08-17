package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NotificationEmailAddressResponse {
    boolean status;
    String message;
    String messageValues;
    String result;
    int code;
}
