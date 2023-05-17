package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.impl.RemoteFileSystem;
import ai.sapper.cdc.core.io.impl.RemoteFileSystemSettings;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public  class S3FileSystemSettings extends RemoteFileSystemSettings {
    public static final String CONFIG_REGION = "region";

    @Config(name = CONFIG_REGION)
    private String region;

    public S3FileSystemSettings() {
        setType(S3FileSystem.class.getCanonicalName());
    }
}
