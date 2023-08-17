package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FileUploadResponse {
    private FileItemKey key;
    private String requestId;
    private String error;
}
