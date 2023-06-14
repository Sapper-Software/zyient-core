package ai.sapper.cdc.core.connections.settings;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Exists;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;


/**
 * <pre>
 *     <connections>
 *         <connection>
 *             <class>[Connection class]</class>
 *             <zookeeper>
 *                  <name>[Connection name, must be unique]</name>
 *                  <connectionString>[ZooKeeper connection string]</connectionString>
 *                  <authenticationHandler>[ZK Authentication handler class (optional)</authenticationHandler>
 *                  <retry>
 *                      <retries>[# of retries, default = 3]</retries>
 *                      <interval>[interval between retries, default = 1sec]</interval>
 *                  </retry>
 *                  <connectionTimeout>[Connection timeout, default = ignore]</connectionTimeout>
 *                  <sessionTimeout>[Session timeout, default = ignore]</sessionTimeout>
 *                  <namespace>[Namespace (optional)</namespace>
 *             </zookeeper>
 *         </connection>
 *     </connections>
 *     ...
 *     <save>[Save connections to ZooKeeper, default=false]</save>
 *     <override>[Override saved connections, default = true]</override>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class ZookeeperSettings extends ConnectionSettings {
    public static final class Constants {
        public static final String CONFIG_CONNECTION = "connectionString";
        public static final String CONFIG_AUTH_HANDLER = "authenticationHandler";
        public static final String CONFIG_RETRY = "retry";
        public static final String CONFIG_RETRY_INTERVAL = "retry.interval";
        public static final String CONFIG_RETRY_TRIES = "retry.retries";
        public static final String CONFIG_CONN_TIMEOUT = "connectionTimeout";
        public static final String CONFIG_SESSION_TIMEOUT = "sessionTimeout";
        public static final String CONFIG_NAMESPACE = "namespace";
        public static final String CONFIG_ZK_CONFIG = "zookeeperConfigFile";
    }

    @Config(name = Constants.CONFIG_CONNECTION)
    private String connectionString;
    @Config(name = Constants.CONFIG_AUTH_HANDLER, required = false)
    private String authenticationHandler;
    @Config(name = Constants.CONFIG_NAMESPACE, required = false)
    private String namespace;
    @Config(name = Constants.CONFIG_RETRY, required = false, type = Exists.class)
    private boolean retryEnabled = false;
    @Config(name = Constants.CONFIG_RETRY_INTERVAL, required = false, type = Integer.class)
    private int retryInterval = 1000;
    @Config(name = Constants.CONFIG_RETRY_TRIES, required = false, type = Integer.class)
    private int retryCount = 3;
    @Config(name = Constants.CONFIG_CONN_TIMEOUT, required = false, type =  Integer.class)
    private int connectionTimeout = -1;
    @Config(name = Constants.CONFIG_SESSION_TIMEOUT, required = false, type = Integer.class)
    private int sessionTimeout = -1;

    public ZookeeperSettings() {
        setType(EConnectionType.zookeeper);
    }

    public ZookeeperSettings(@NonNull ConnectionSettings settings) {
        super(settings);
        Preconditions.checkArgument(settings instanceof ZookeeperSettings);
        this.connectionString = ((ZookeeperSettings) settings).getConnectionString();
        this.authenticationHandler = ((ZookeeperSettings) settings).getAuthenticationHandler();
        this.namespace = ((ZookeeperSettings) settings).getNamespace();
        this.retryEnabled = ((ZookeeperSettings) settings).isRetryEnabled();
        this.retryInterval = ((ZookeeperSettings) settings).getRetryInterval();
        this.retryCount = ((ZookeeperSettings) settings).getRetryCount();
        this.connectionTimeout = ((ZookeeperSettings) settings).getConnectionTimeout();
        this.sessionTimeout = ((ZookeeperSettings) settings).getSessionTimeout();
    }
}
