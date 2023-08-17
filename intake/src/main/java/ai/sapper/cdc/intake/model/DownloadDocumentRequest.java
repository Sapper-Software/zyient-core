package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DownloadDocumentRequest {
    private String r_object_id;
    private String filename_1;
   // private String document_uri;


    @Override
    public String toString() {
        return "DownloadDocumentRequest{" +
                "r_object_id='" + r_object_id + '\'' +
                ", filename_1='" + filename_1 + '\'' +
                '}';
    }
}
