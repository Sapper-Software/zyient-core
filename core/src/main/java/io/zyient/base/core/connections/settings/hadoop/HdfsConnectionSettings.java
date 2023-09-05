/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.base.core.connections.settings.hadoop;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.Config;
import io.zyient.base.common.config.Settings;
import io.zyient.base.core.connections.settings.ConnectionSettings;
import io.zyient.base.core.connections.settings.EConnectionType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HdfsConnectionSettings {
    public static final class Constants {
        public static final String CONN_PRI_NAME_NODE_URI = "namenode.URI.primary";
        public static final String CONN_SEC_NAME_NODE_URI = "namenode.URI.secondary";
        public static final String CONN_SECURITY_ENABLED = "security.enabled";
        public static final String CONN_ADMIN_CLIENT_ENABLED = "enableAdmin";
    }

    /**
     * <pre>
     *     <connections>
     *         <connection>
     *             <class>[Connection class]</class>
     *             <hdfs(-ha)>
     *                  <name>[Connection name, must be unique]</name>
     *                  <security>
     *                      <enabled>[Enable secured connection, default = false]</enabled>
     *                      ...
     *                  </security>
     *                  <enableAdmin>[Enable NameNode Admin connection, default = false]</enableAdmin>
     *             </hdfs(-ha)>
     *         </connection>
     *     </connections>
     * </pre>
     */
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
            securitySettings = ((HdfsBaseSettings) settings).securitySettings;
        }
    }

    /**
     * <pre>
     *     <connections>
     *         <connection>
     *             <class>[Connection class]</class>
     *             <hdfs>
     *                  <name>[Connection name, must be unique]</name>
     *                  <security>
     *                      <enabled>[Enable secured connection, default = false]</enabled>
     *                      ...
     *                  </security>
     *                  <enableAdmin>[Enable NameNode Admin connection, default = false]</enableAdmin>
     *                  <namenode>
     *                          <URI>
     *                              <primary>[Primary NameNode URI]</primary>
     *                              <secondary>[Primary NameNode URI (Optional)]</secondary>
     *                          </URI>
     *                  </namenode>
     *             </hdfs>
     *         </connection>
     *     </connections>
     * </pre>
     */
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

        }

        public HdfsSettings(@NonNull ConnectionSettings settings) {
            super(settings);
            Preconditions.checkArgument(settings instanceof HdfsSettings);
            primaryNameNodeUri = ((HdfsSettings) settings).primaryNameNodeUri;
            secondaryNameNodeUri = ((HdfsSettings) settings).secondaryNameNodeUri;
        }
    }


    /**
     * <pre>
     *     <connections>
     *         <connection>
     *             <class>[Connection class]</class>
     *             <hdfs-ha>
     *                  <name>[Connection name, must be unique]</name>
     *                  <security>
     *                      <enabled>[Enable secured connection, default = false]</enabled>
     *                      ...
     *                  </security>
     *                  <enableAdmin>[Enable NameNode Admin connection, default = false]</enableAdmin>
     *                  <nameservice>[NameNode NameService name]</nameservice>
     *                  <failoverProvider>[NameNode Failover Provider class (org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider)]</failoverProvider>
     *                  <namenodes>[Comma seperated list of NameNode URLs (nn1=192.168.2.2:9820;nn2=192.168.2.3:9820)</namenodes>
     *             </hdfs-ha>
     *         </connection>
     *     </connections>
     * </pre>
     */
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

        }

        public HdfsHASettings(@NonNull ConnectionSettings settings) {
            super(settings);
            Preconditions.checkArgument(settings instanceof HdfsHASettings);
            this.nameService = ((HdfsHASettings) settings).nameService;
            this.failoverProvider = ((HdfsHASettings) settings).failoverProvider;
            this.nameNodeAddresses = ((HdfsHASettings) settings).nameNodeAddresses;
        }
    }

    /**
     * TODO: Configuration not tested
     * <pre>
     *     <security>
     *         <enabled>[Enable secured connection, default = false]</enabled>
     *         <hadoop>
     *             <security></security>
     *         </hadoop>
     *     </security>
     * </pre>
     */
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
