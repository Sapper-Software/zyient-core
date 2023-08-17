package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;

/**
 * Class representing request to the bayer ecws api for updating the source document
 */
@Getter
@Setter
public class UpdateSDRADocumentRequest {
    private String r_object_id;//mandatory field
    private String controlling_app;
    private String country;
    private String language;
    private String r_object_type;
    private String document_category;
    private String initiate_date;
    private String bayer_initial_date;
    private String filename_1;
    private String fileContent_1;
    private String[] central_case_num;
    private String remarks;

    @Override
    public String toString() {
        return "UpdateSDRADocumentRequest{" +
                "r_object_id='" + r_object_id + '\'' +
                ", controlling_app='" + controlling_app + '\'' +
                ", country='" + country + '\'' +
                ", language='" + language + '\'' +
                ", r_object_type='" + r_object_type + '\'' +
                ", document_category='" + document_category + '\'' +
                ", initiate_date='" + initiate_date + '\'' +
                ", bayer_initial_date='" + bayer_initial_date + '\'' +
                ", filename_1='" + filename_1 + '\'' +
                ", central_case_num='" + Arrays.toString(central_case_num) + '\'' +
                ", remarks='" + remarks + '\'' +
                '}';
    }
}
