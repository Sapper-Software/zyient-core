package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.io.impl.RemoteFileSystemSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public  class AzureFileSystemSettings extends RemoteFileSystemSettings {
    private AzureFsClientSettings clientSettings;

    public AzureFileSystemSettings() {
        setType(AzureFileSystem.class.getCanonicalName());
    }
}