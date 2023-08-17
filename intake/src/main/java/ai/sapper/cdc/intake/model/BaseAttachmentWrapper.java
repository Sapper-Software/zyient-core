package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BaseAttachmentWrapper {
    private EContentSource source;
    private String path;
}
