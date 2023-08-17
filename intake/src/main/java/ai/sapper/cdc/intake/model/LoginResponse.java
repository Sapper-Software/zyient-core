package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LoginResponse {
    private String authcode;
    private String status;
}