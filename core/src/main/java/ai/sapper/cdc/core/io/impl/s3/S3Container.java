package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.Container;
import ai.sapper.cdc.core.io.model.InodeType;
import ai.sapper.cdc.core.io.model.PathInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class S3Container extends Container {
    @Config(name = "bucket")
    private String bucket;

    @Override
    public PathInfo pathInfo(@NonNull FileSystem fs) {
        return new S3PathInfo(fs, getDomain(), getBucket(), getPath(), InodeType.Directory);
    }
}
