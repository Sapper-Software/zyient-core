package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UploadSDRADocumentRequest {
    private String controlling_app;
    private String country;
    private String language;
    private String r_object_type;
    private String document_category;
    private String initial_date;
    private String bayer_initial_date;
    private String filename_1;
    private String fileContent_1;
    private String[] central_case_num;
    private String remarks;

    @Override
    public String toString() {
        return "UploadSDRADocumentRequest{" +
                "controlling_app='" + controlling_app + '\'' +
                ", country='" + country + '\'' +
                ", language='" + language + '\'' +
                ", r_object_type='" + r_object_type + '\'' +
                ", document_category='" + document_category + '\'' +
                ", initial_date='" + initial_date + '\'' +
                ", bayer_initial_date='" + bayer_initial_date + '\'' +
                ", filename_1='" + filename_1 + '\'' +
                ", central_case_num='" + central_case_num + '\'' +
                ", remarks='" + remarks + '\'' +
                '}';
    }
}
