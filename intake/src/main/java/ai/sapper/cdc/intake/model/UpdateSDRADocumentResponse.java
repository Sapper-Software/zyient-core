package ai.sapper.cdc.intake.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UpdateSDRADocumentResponse {
    private String authcode;
    @JsonProperty("@status")
    private String status;
}
