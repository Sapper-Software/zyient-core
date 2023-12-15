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

package io.zyient.base.core.keystore;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.ChecksumUtils;
import io.zyient.base.common.utils.CypherUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.connections.ConnectionManager;
import io.zyient.base.core.connections.db.JdbcConnection;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DbKeyStore extends KeyStore {
    public static final String DB_TABLE_NAME = "zy_keystore";
    public static final String DB_COLUMN_NAMESPACE = "key_namespace";
    public static final String DB_COLUMN_NAME = "key_name";
    public static final String DB_COLUMN_VALUE = "key_value";
    public static final String DB_COLUMN_TIMESTAMP = "key_timestamp";
    public static final String CONFIG_NAME = "name";
    public static final String CONFIG_IV_SPEC = "iv";

    private static class KeyRecord {
        private String namespace;
        private String name;
        private String value;
        private long timestamp;
        private boolean isNew;
    }

    private String name;
    private String iv;
    private JdbcConnection connection;
    private HierarchicalConfiguration<ImmutableNode> config;
    private String passwdHash;
    private BaseEnv<?> env;
    private long syncTimestamp;
    private final Map<String, KeyRecord> records = new HashMap<>();

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                     @NonNull String password,
                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            String pwd = password;
            config = configNode.configurationAt(__CONFIG_PATH);
            Preconditions.checkNotNull(config);
            name = config.getString(CONFIG_NAME);
            ConfigReader.checkStringValue(name, getClass(), CONFIG_NAME);
            password = CypherUtils.checkPassword(password, name);
            withPassword(password);

            HierarchicalConfiguration<ImmutableNode> dbc
                    = config.configurationAt(ConnectionManager.Constants.CONFIG_CONNECTION_LIST);
            connection = (JdbcConnection) new JdbcConnection()
                    .withPassword(pwd);
            connection
                    .init(dbc, env)
                    .connect();

            iv = config.getString(CONFIG_IV_SPEC);
            ConfigReader.checkStringValue(iv, getClass(), CONFIG_IV_SPEC);
            iv = CypherUtils.formatIvString(iv);
            passwdHash = ChecksumUtils.generateHash(password);
            checkDbSetup();
            this.env = env;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private synchronized void checkDbSetup() throws Exception {
        try (Connection connection = this.connection.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            boolean found = false;
            try (ResultSet rs = metaData.getTables(null, null, null, new String[]{"TABLE"})) {
                while (rs.next()) {
                    String name = rs.getString("TABLE_NAME");
                    if (DB_TABLE_NAME.compareToIgnoreCase(name) == 0) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found)
                createDbTable(connection);
            fetchRecords(connection);
        }
    }

    private void createDbTable(Connection connection) throws Exception {
        String sql = String.format("CREATE TABLE %s (%s VARCHAR(128) NOT NULL," +
                        " %s VARCHAR(128) NOT NULL, " +
                        "%s VARCHAR(4096) NOT NULL, " +
                        "%s NUMERIC(16,0), " +
                        "PRIMARY KEY (%s, %s))",
                DB_TABLE_NAME,
                DB_COLUMN_NAMESPACE,
                DB_COLUMN_NAME,
                DB_COLUMN_VALUE,
                DB_COLUMN_TIMESTAMP,
                DB_COLUMN_NAMESPACE,
                DB_COLUMN_NAME);
        DefaultLogger.debug(String.format("CREATING TABLE [%s]", sql));
        try (Statement stmnt = connection.createStatement()) {
            stmnt.execute(sql);
        }
    }

    private void fetchRecords(Connection connection) throws Exception {
        records.clear();
        String sql = String.format("SELECT %s, %s, %s, %s FROM %s WHERE %s = ?",
                DB_COLUMN_NAMESPACE,
                DB_COLUMN_NAME,
                DB_COLUMN_VALUE,
                DB_COLUMN_TIMESTAMP,
                DB_TABLE_NAME,
                DB_COLUMN_NAMESPACE);
        try (PreparedStatement pstmnt = connection.prepareStatement(sql)) {
            pstmnt.setString(1, name);
            try (ResultSet rs = pstmnt.executeQuery()) {
                while (rs.next()) {
                    KeyRecord record = new KeyRecord();
                    record.namespace = rs.getString(1);
                    record.name = rs.getString(2);
                    record.value = rs.getString(3);
                    record.timestamp = rs.getLong(4);
                    record.isNew = false;

                    records.put(record.name, record);
                }
            }
        }
        syncTimestamp = System.nanoTime();
    }

    @Override
    public void save(@NonNull String name,
                     @NonNull String value,
                     @NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        synchronized (records) {
            KeyRecord record = null;
            if (records.containsKey(name)) {
                record = records.get(name);
            } else {
                record = new KeyRecord();
                record.namespace = this.name;
                record.name = name;
                record.isNew = true;
                records.put(name, record);
            }
            record.value = CypherUtils.encryptAsString(value, password, iv);
            record.timestamp = System.nanoTime();
        }
    }

    @Override
    public String read(@NonNull String name,
                       @NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        if (records.containsKey(name)) {
            KeyRecord record = records.get(name);
            byte[] data = CypherUtils.decrypt(record.value, password, iv);
            return new String(data, StandardCharsets.UTF_8);
        }
        return null;
    }

    @Override
    public void delete(@NonNull String name,
                       @NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        synchronized (records) {
            if (records.containsKey(name)) {
                records.remove(name);
                try (Connection connection = this.connection.getConnection()) {
                    String sql = String.format("DELETE FROM %s WHERE %s = ? AND %s = ?",
                            DB_TABLE_NAME,
                            DB_COLUMN_NAMESPACE,
                            DB_COLUMN_NAME);
                    try (PreparedStatement pstmnt = connection.prepareStatement(sql)) {
                        pstmnt.setString(1, this.name);
                        pstmnt.setString(2, name);
                        int r = pstmnt.executeUpdate();
                        if (r == 0) {
                            DefaultLogger.warn(String.format("[%s] Specified key not found. [key=%s]", this.name, name));
                        } else {
                            DefaultLogger.info(String.format("[%s] Deleted key. [key=%s]", this.name, name));
                        }
                    }
                }
            }
        }
    }

    @Override
    public void delete(@NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        synchronized (records) {
            try (Connection connection = this.connection.getConnection()) {
                String sql = String.format("DELETE FROM %s WHERE %s = ? ",
                        DB_TABLE_NAME,
                        DB_COLUMN_NAMESPACE
                );
                try (PreparedStatement pstmnt = connection.prepareStatement(sql)) {
                    pstmnt.setString(1, this.name);
                    int r = pstmnt.executeUpdate();
                    DefaultLogger.warn(String.format("[%s] Deleted all keys. [count=%d]", name, r));
                }
            }
            records.clear();
        }
    }

    @Override
    public String flush(@NonNull String password) throws Exception {
        Preconditions.checkState(connection != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        synchronized (records) {
            try (Connection connection = this.connection.getConnection()) {
                for (String key : records.keySet()) {
                    KeyRecord record = records.get(key);
                    if (record.timestamp > syncTimestamp) {
                        if (record.isNew) {
                            insertKey(record, connection);
                            record.isNew = false;
                        } else {
                            updateKey(record, connection);
                        }
                    }
                }
            }
            syncTimestamp = System.nanoTime();
            return name;
        }
    }

    private void insertKey(KeyRecord record, Connection connection) throws Exception {
        String sql = String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
                DB_TABLE_NAME,
                DB_COLUMN_NAMESPACE,
                DB_COLUMN_NAME,
                DB_COLUMN_VALUE,
                DB_COLUMN_TIMESTAMP);
        DefaultLogger.debug(String.format("[%s] Add KEY SQL [%s]", name, sql));
        try (PreparedStatement pstmnt = connection.prepareStatement(sql)) {
            pstmnt.setString(1, this.name);
            pstmnt.setString(2, record.name);
            pstmnt.setString(3, record.value);
            pstmnt.setLong(4, record.timestamp);

            int r = pstmnt.executeUpdate();
            if (r != 0) {
                DefaultLogger.info(String.format("[%s] Inserted new key. [key=%s]", name, record.name));
            } else {
                DefaultLogger.warn(String.format("[%s] Insert returned zero. [key=%s]", name, record.name));
            }
        }
    }

    private void updateKey(KeyRecord record, Connection connection) throws Exception {
        String sql = String.format("UPDATE %s SET %s = ?, %s = ? WHERE %s = ? AND %s = ?",
                DB_TABLE_NAME,
                DB_COLUMN_VALUE,
                DB_COLUMN_TIMESTAMP,
                DB_COLUMN_NAMESPACE,
                DB_COLUMN_NAME);
        DefaultLogger.debug(String.format("[%s] Update KEY SQL [%s]", name, sql));
        try (PreparedStatement pstmnt = connection.prepareStatement(sql)) {
            pstmnt.setString(1, record.value);
            pstmnt.setLong(2, record.timestamp);
            pstmnt.setString(3, name);
            pstmnt.setString(4, record.name);

            int r = pstmnt.executeUpdate();
            if (r != 0) {
                DefaultLogger.info(String.format("[%s] Updated key. [key=%s]", name, record.name));
            } else {
                DefaultLogger.warn(String.format("[%s] Update returned zero. [key=%s]", name, record.name));
            }
        }
    }


    @Override
    public void close() throws IOException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
