package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.core.io.impl.local.LocalContainer;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class S3Container extends LocalContainer {
    private String bucket;
}
