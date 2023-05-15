package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.connections.WebServiceConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class WebServiceConnectionSettings extends ConnectionSettings {

    public static class Constants {
        public static final String CONFIG_URL = "endpoint";
    }

    @Config(name = Constants.CONFIG_URL)
    private String endpoint;

    public WebServiceConnectionSettings() {
        super(WebServiceConnection.class);
        setType(EConnectionType.rest);
    }

    public WebServiceConnectionSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof WebServiceConnectionSettings);
        this.endpoint = ((WebServiceConnectionSettings) settings).getEndpoint();
    }
}
