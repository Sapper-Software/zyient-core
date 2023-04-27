package ai.sapper.cdc.common.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public abstract class Settings {
    public static final String CONFIG_PARAMS = "parameters";

    @Config(name = CONFIG_PARAMS, required = false, type = Map.class)
    private Map<String, String> properties;

    public Settings() {

    }

    public Settings(@NonNull Settings source) {
        this.properties = source.getProperties();
    }
}
