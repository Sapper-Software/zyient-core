package ai.sapper.cdc.intake.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UploadSDRADocumentResponse {
    private String url;
    private String authcode;
    private String r_object_id;
    @JsonProperty("@status")
    private String status;


}
