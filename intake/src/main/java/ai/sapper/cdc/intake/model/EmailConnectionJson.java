package ai.sapper.cdc.intake.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class EmailConnectionJson {
    @Nonnull
    private String serverId;
    @Nonnull
    private String name;
    @Nonnull
    private String emailId;
    @Nonnull
    private String channel;
    private String domain;
    @Nonnull
    private String account;
    @Nonnull
    private short partition;
    @Nonnull
    private String password;
    private String proxyUser;
    private String delegateUser;
    @Nonnull
    private String emailType;
    private String tenantId;

    @Override
    public String toString() {
        return String.format("serverId: %s," +
                "name: %s," +
                "emailId: %s," +
                "channel: %s," +
                "domain: %s," +
                "account: %s," +
                "proxyUser: %s," +
                "delegateUser: %s," +
                "emailType : %s," +
                "tenantId : %s", serverId,name,emailId,channel,domain,account,proxyUser,delegateUser,emailType,tenantId);
    }
}
