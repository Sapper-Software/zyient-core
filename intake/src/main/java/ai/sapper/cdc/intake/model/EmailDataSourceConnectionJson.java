package ai.sapper.cdc.intake.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class EmailDataSourceConnectionJson {
    private List<EmailConnectionJson> emailConnections;

    @Override
    public String toString() {
        return "EmailDataSourceConnectionJson{" +
                "emailConnections=" + emailConnections +
                '}';
    }
}
