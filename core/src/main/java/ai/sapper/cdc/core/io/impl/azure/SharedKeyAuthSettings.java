package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.Config;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SharedKeyAuthSettings extends AzureStorageAuthSettings {
    public static final String CONFIG_AUTH_KEY = "authKey";

    @Config(name = CONFIG_AUTH_KEY)
    private String authKey;
}