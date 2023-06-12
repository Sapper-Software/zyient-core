package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.ArchivePathInfo;
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
public class AzureContainer extends Container {
    @Config(name = "container")
    private String container;

    @Override
    public PathInfo pathInfo(@NonNull FileSystem fs) {
        return new AzurePathInfo(fs, getDomain(), getContainer(), getPath(), InodeType.Directory);
    }

    @Override
    public ArchivePathInfo pathInfo(@NonNull Archiver archiver) {
        return null;
    }
}
