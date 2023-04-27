package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.connections.hadoop.HdfsHAConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HdfsConnectionSettings {
    public static final class Constants {
        public static final String CONN_PRI_NAME_NODE_URI = "namenode.primary.URI";
        public static final String CONN_SEC_NAME_NODE_URI = "namenode.secondary.URI";
        public static final String CONN_SECURITY_ENABLED = "security.enabled";
        public static final String CONN_ADMIN_CLIENT_ENABLED = "enableAdmin";
    }

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public abstract static class HdfsBaseSettings extends ConnectionSettings {
        @Config(name = HdfsConnectionSettings.Constants.CONN_SECURITY_ENABLED, required = false, type = Boolean.class)
        protected boolean securityEnabled = false;
        @Config(name = HdfsConnectionSettings.Constants.CONN_ADMIN_CLIENT_ENABLED, required = false, type = Boolean.class)
        protected boolean adminEnabled = false;
        protected HdfsSecuritySettings securitySettings;

        public HdfsBaseSettings() {
            setType(EConnectionType.hadoop);
        }

        public HdfsBaseSettings(@NonNull ConnectionSettings settings) {
            super(settings);
            Preconditions.checkArgument(settings instanceof HdfsBaseSettings);
            securityEnabled = ((HdfsBaseSettings) settings).securityEnabled;
            adminEnabled = ((HdfsBaseSettings) settings).adminEnabled;
        }
    }

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsSettings extends HdfsBaseSettings {
        @Config(name = HdfsConnectionSettings.Constants.CONN_PRI_NAME_NODE_URI)
        private String primaryNameNodeUri;
        @Config(name = HdfsConnectionSettings.Constants.CONN_SEC_NAME_NODE_URI)
        private String secondaryNameNodeUri;

        public HdfsSettings() {
            setConnectionClass(HdfsConnection.class);
        }

        public HdfsSettings(@NonNull ConnectionSettings settings) {
            super(settings);
            Preconditions.checkArgument(settings instanceof HdfsSettings);
            primaryNameNodeUri = ((HdfsSettings) settings).primaryNameNodeUri;
            secondaryNameNodeUri = ((HdfsSettings) settings).secondaryNameNodeUri;
        }
    }


    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsHASettings extends HdfsBaseSettings {
        public static class Constants {
            public static final String DFS_NAME_SERVICES = "nameservice";
            public static final String DFS_FAILOVER_PROVIDER = "failoverProvider";
            public static final String DFS_NAME_NODES = "namenodes";
        }

        @Config(name = Constants.DFS_NAME_SERVICES)
        private String nameService;
        @Config(name = Constants.DFS_FAILOVER_PROVIDER)
        private String failoverProvider;
        @Config(name = Constants.DFS_NAME_NODES, parser = HdfsUrlParser.class)
        private String[][] nameNodeAddresses;

        public HdfsHASettings() {
            setConnectionClass(HdfsHAConnection.class);
        }
    }

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
            property = "@class")
    public static class HdfsSecuritySettings extends Settings {
        public static final String __CONFIG_PATH = "security";
        private static final String HDFS_PARAM_SECURITY_REQUIRED = "hadoop.security.authorization";
        private static final String HDFS_PARAM_SECURITY_MODE = "hadoop.security.authentication";
        private static final String HDFS_PARAM_SECURITY_KB_HOST = "java.security.krb5.kdc";
        private static final String HDFS_PARAM_SECURITY_KB_REALM = "java.security.krb5.realm";
        private static final String HDFS_PARAM_SECURITY_PRINCIPLE = "dfs.namenode.kerberos.principal.pattern";
        private static final String HDFS_SECURITY_TYPE = "kerberos";
        private static final String HDFS_SECURITY_USERNAME = "kerberos.username";
        private static final String HDFS_SECURITY_KEYTAB = "kerberos.user.keytab";

        public HdfsSecuritySettings() {

        }

        public HdfsSecuritySettings(@NonNull Settings settings) {
            super(settings);
            Preconditions.checkArgument(settings instanceof HdfsSecuritySettings);
        }

        public void setup(@NonNull Configuration conf) throws ConfigurationException, IOException {
            Preconditions.checkState(getParameters() != null);


            // set kerberos host and realm
            System.setProperty(HDFS_PARAM_SECURITY_KB_HOST, getParameters().get(HDFS_PARAM_SECURITY_KB_HOST));
            System.setProperty(HDFS_PARAM_SECURITY_KB_REALM, getParameters().get(HDFS_PARAM_SECURITY_KB_REALM));

            conf.set(HDFS_PARAM_SECURITY_MODE, HDFS_SECURITY_TYPE);
            conf.set(HDFS_PARAM_SECURITY_REQUIRED, "true");

            String principle = getParameters().get(HDFS_PARAM_SECURITY_PRINCIPLE);
            if (!Strings.isNullOrEmpty(principle)) {
                conf.set(HDFS_PARAM_SECURITY_PRINCIPLE, principle);
            }
            String username = getParameters().get(HDFS_SECURITY_USERNAME);
            if (Strings.isNullOrEmpty(username)) {
                throw new ConfigurationException(String.format("Missing Kerberos user. [param=%s]", HDFS_SECURITY_USERNAME));
            }
            String keyteabf = getParameters().get(HDFS_SECURITY_KEYTAB);
            if (Strings.isNullOrEmpty(keyteabf)) {
                throw new ConfigurationException(String.format("Missing keytab path. [param=%s]", HDFS_SECURITY_KEYTAB));
            }
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(username, keyteabf);
        }
    }
}
