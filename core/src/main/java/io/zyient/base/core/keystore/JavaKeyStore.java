/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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
import io.zyient.base.common.utils.ChecksumUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.keystore.settings.JavaKeyStoreSettings;
import io.zyient.base.core.keystore.settings.KeyStoreSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;


public class JavaKeyStore extends KeyStore {

    private java.security.KeyStore store;
    private String passwdHash;

    public JavaKeyStore() {
        super(JavaKeyStoreSettings.class);
    }

    @Override
    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                     @NonNull String password,
                     @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            Preconditions.checkNotNull(settings());
            JavaKeyStoreSettings settings = (JavaKeyStoreSettings) settings();
            passwdHash = ChecksumUtils.generateHash(password);
            File kf = new File(settings.getKeyStoreFile());
            if (!kf.exists()) {
                createEmptyStore(kf.getAbsolutePath(), password);
            } else {
                store = java.security.KeyStore.getInstance(settings.getKeyStoreType());
                try (FileInputStream fis = new FileInputStream(kf)) {
                    store.load(fis, password.toCharArray());
                }
            }
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public File filePath(@NonNull String key) throws Exception {
        Preconditions.checkNotNull(settings());
        JavaKeyStoreSettings settings = (JavaKeyStoreSettings) settings();
        if (!authenticate(key)) {
            throw new Exception("Authentication failed...");
        }
        File file = new File(settings.getKeyStoreFile());
        if (file.exists()) {
            return file;
        }
        throw new Exception("KeyStore file not found.");
    }

    private void createEmptyStore(String path, String password) throws Exception {
        JavaKeyStoreSettings settings = (JavaKeyStoreSettings) settings();
        store = java.security.KeyStore.getInstance(settings.getKeyStoreType());
        store.load(null, password.toCharArray());

        // Save the keyStore
        try (FileOutputStream fos = new FileOutputStream(path)) {
            store.store(fos, password.toCharArray());
        }
        save(KeyStoreSettings.DEFAULT_KEY, password);
        flush(password);
    }

    @Override
    public void save(@NonNull String name,
                     @NonNull String value,
                     @NonNull String password) throws Exception {
        Preconditions.checkNotNull(store);
        Preconditions.checkState(!Strings.isNullOrEmpty(passwdHash));
        JavaKeyStoreSettings settings = (JavaKeyStoreSettings) settings();

        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        java.security.KeyStore.SecretKeyEntry secret
                = new java.security.KeyStore.SecretKeyEntry(generate(value, settings.getCipherAlgo()));
        java.security.KeyStore.ProtectionParameter parameter
                = new java.security.KeyStore.PasswordProtection(password.toCharArray());
        store.setEntry(name, secret, parameter);
    }

    @Override
    public String read(@NonNull String name,
                       @NonNull String password) throws Exception {
        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        Preconditions.checkNotNull(store);
        JavaKeyStoreSettings settings = (JavaKeyStoreSettings) settings();

        java.security.KeyStore.ProtectionParameter param
                = new java.security.KeyStore.PasswordProtection(password.toCharArray());
        if (store.containsAlias(name)) {
            java.security.KeyStore.SecretKeyEntry key
                    = (java.security.KeyStore.SecretKeyEntry) store.getEntry(name, param);
            return extractValue(key.getSecretKey(), settings.getCipherAlgo());
        }
        return null;
    }

    @Override
    public void delete(@NonNull String name,
                       @NonNull String password) throws Exception {
        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        Preconditions.checkNotNull(store);
        store.deleteEntry(name);
    }

    @Override
    public void delete(@NonNull String password) throws Exception {
        String hash = ChecksumUtils.generateHash(password);
        Preconditions.checkArgument(hash.equals(passwdHash));
        Preconditions.checkNotNull(store);
        JavaKeyStoreSettings settings = (JavaKeyStoreSettings) settings();

        Enumeration<String> aliases = store.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            store.deleteEntry(alias);
        }
        store = null;

        Files.delete(Paths.get(settings.getKeyStoreType()));
    }

    @Override
    public String flush(@NonNull String password) throws Exception {
        Preconditions.checkNotNull(store);
        JavaKeyStoreSettings settings = (JavaKeyStoreSettings) settings();
        try (FileOutputStream fos = new FileOutputStream(settings.getKeyStoreFile(), false)) {
            store.store(fos, password.toCharArray());
        }
        return settings.getKeyStoreType();
    }

    @Override
    public void close() throws IOException {

    }
}
