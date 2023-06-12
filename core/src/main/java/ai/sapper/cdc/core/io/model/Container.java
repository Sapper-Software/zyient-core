package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class Container extends Settings {
    public static final String PATH_ROOT = "root";

    @Config(name = "domain")
    private String domain;
    @Config(name = "path")
    private String path;

    public abstract PathInfo pathInfo(@NonNull FileSystem fs);

    public abstract ArchivePathInfo pathInfo(@NonNull Archiver archiver);
}
