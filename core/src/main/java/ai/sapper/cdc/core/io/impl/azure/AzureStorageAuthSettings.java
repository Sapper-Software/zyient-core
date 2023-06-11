package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.Settings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class AzureStorageAuthSettings extends Settings {
    public static final String __CONFIG_PATH = "auth";
}
