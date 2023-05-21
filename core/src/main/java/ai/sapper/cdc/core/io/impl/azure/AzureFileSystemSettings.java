package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.impl.RemoteFileSystemSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public  class AzureFileSystemSettings extends RemoteFileSystemSettings {
    @Config(name = "uploadTimeout", required = false, type = Long.class)
    private long uploadTimeout = 15; // 15 Seconds
    private AzureFsClientSettings clientSettings;

    public AzureFileSystemSettings() {
        setType(AzureFileSystem.class.getCanonicalName());
    }
}