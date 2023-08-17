package ai.sapper.cdc.intake.model;

import com.codekutter.common.model.ConnectionConfig;
import com.codekutter.common.stores.annotations.Encrypted;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.persistence.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Getter
@Setter
@Entity
@Table(name = "config_mail_connections")
public class MailConnectionDbConfig extends ConnectionConfig {
    @Enumerated(EnumType.STRING)
    @Column(name = "type")
    private EMailServerType type;
    @Column(name = "host", nullable = false)
    private String host;
    @Column(name = "port")
    private int port;
    @Column(name = "domain")
    private String domain;
    @Column(name = "username", nullable = false)
    private String username;
    @Column(name = "password", nullable = false)
    @Encrypted
    private String password;
    @Column(name = "tenant_id")
    private String tenantId;
    @Column(name = "request_time_out")
    private int requestTimeout;
    @Column(name = "proxy_user")
    private String proxyUser;
    @Column(name = "delegate_user")
    private String delegateUser;
    @NaturalId
    @Column(name = "email_id", nullable = false)
    private String emailId;
    @Column(name = "use_ssl")
    private boolean useSSL;
    @Column(name = "use_tls")
    private boolean useTLS;
    @Column(name = "parameters")
    private String optionString;
    @Transient
    private Map<String, String> options;

    @Override
    public void postLoad() throws ConfigurationException {
        if (!Strings.isNullOrEmpty(optionString)) {
            String[] ops = optionString.split(";");
            if (ops.length > 0) {
                options = new HashMap<>();
                for(String op : ops) {
                    if (!Strings.isNullOrEmpty(op)) {
                        String[] parts = op.split("=");
                        if (parts.length == 2) {
                            String key = parts[0];
                            if (!Strings.isNullOrEmpty(key)) {
                                options.put(key, parts[1]);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MailConnectionDbConfig config = (MailConnectionDbConfig) o;
        return port == config.port &&
                useSSL == config.useSSL &&
                useTLS == config.useTLS &&
                type == config.type &&
                Objects.equals(host, config.host) &&
                Objects.equals(username, config.username) &&
                Objects.equals(password, config.password) &&
                Objects.equals(emailId, config.emailId) &&
                Objects.equals(optionString, config.optionString) &&
                Objects.equals(options, config.options) &&
                Objects.equals(requestTimeout, config.requestTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, host, port, username, password, emailId, useSSL, useTLS, optionString, options, requestTimeout);
    }
}
