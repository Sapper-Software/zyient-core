package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.ArchivePathInfo;
import ai.sapper.cdc.core.io.model.Container;
import ai.sapper.cdc.core.io.model.PathInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class LocalContainer extends Container {

    @Override
    public PathInfo pathInfo(@NonNull FileSystem fs) {
        return new LocalPathInfo(fs, getPath(), getDomain());
    }

    @Override
    public ArchivePathInfo pathInfo(@NonNull Archiver archiver) {
        throw new RuntimeException("Method should not be called...");
    }
}
