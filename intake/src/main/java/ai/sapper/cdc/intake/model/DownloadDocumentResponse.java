package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DownloadDocumentResponse {
    private String authcode;
    private String object_name;
    private String wf_status;
    private String fileContent_1;
    private String filename_1;
    private String remarks;
}
