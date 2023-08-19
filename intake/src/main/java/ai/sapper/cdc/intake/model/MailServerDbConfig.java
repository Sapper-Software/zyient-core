package ai.sapper.cdc.intake.model;

import com.codekutter.common.model.ConnectionConfig;
import com.codekutter.zconfig.common.ConfigurationException;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Getter
@Setter
@Entity
@Table(name = "config_mail_servers")
public class MailServerDbConfig extends ConnectionConfig {
    @NaturalId
    @Column(name = "serverid", nullable = false)
    private String serverId;
    @Enumerated(EnumType.STRING)
    @Column(name = "type")
    private EMailServerType type;
    @Column(name = "host", nullable = false)
    private String host;
    @Column(name = "port")
    private int port;
    @Column(name = "datastore_type", nullable = false)
    private String datastoreType;
    @Column(name = "default_folder", nullable = false)
    private String defaultFolder;
    @Column(name = "default_fetch_count", nullable = false)
    private int defaultFetchCount;
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
        MailServerDbConfig config = (MailServerDbConfig) o;
        return port == config.port &&
                useSSL == config.useSSL &&
                useTLS == config.useTLS &&
                type == config.type &&
                Objects.equals(host, config.host) &&
                Objects.equals(optionString, config.optionString) &&
                Objects.equals(options, config.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, host, port, useSSL, useTLS, optionString, options);
    }
}
