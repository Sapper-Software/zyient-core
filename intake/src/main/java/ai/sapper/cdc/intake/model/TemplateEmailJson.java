package ai.sapper.cdc.intake.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class TemplateEmailJson extends EmailJson {
    private String template;
    private Map<String, String> context;

    public void addContext(@Nonnull String key, @Nonnull String value) {
        if (context == null) {
            context = new HashMap<>();
        }
        context.put(key, value);
    }
}
