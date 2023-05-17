package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.core.io.model.FileSystemSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public  class LocalFileSystemSettings extends FileSystemSettings {
    public LocalFileSystemSettings() {
        setType(LocalFileSystem.class.getCanonicalName());
    }
}
